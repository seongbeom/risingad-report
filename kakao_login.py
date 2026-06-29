"""카카오모먼트 비즈니스 토큰 발급/갱신 도구 (로컬, 월 1회).

더블클릭하면 브라우저에 카카오 동의화면이 뜹니다.
→ (광고주 광고계정에 접근권한 있는) 카카오계정으로 로그인 + 광고계정 동의
→ 비즈니스 토큰(access + refresh) 저장 → EC2 업로드.

비즈니스 토큰 refresh_token 유효기간 ~2개월 → 월 1회 갱신 권장.
access_token 은 짧음(수집기가 매 실행 시 refresh 로 자동 재발급).

사용: 카카오모먼트_로그인.command 더블클릭
"""
import datetime
import json
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).parent
DATA = ROOT / "data"
APP = DATA / "kakao_app.json"
TOKEN = DATA / "kakao_token.json"
META = DATA / "kakao_token_meta.json"

# EC2 업로드 (criteo_login.py 와 동일)
KEY = Path.home() / "cafe24_migration" / "cafe24-new-key.pem"
HOST = "ubuntu@52.79.112.252"
REMOTE_DATA = "/opt/cafe24/data"

AUTH_URL = "https://kauth.kakao.com/oauth/business/authorize"
TOKEN_URL = "https://kauth.kakao.com/oauth/business/token"
TOKENINFO_URL = "https://kapi.kakao.com/v1/business/tokeninfo"

_captured = {}


class _Handler(BaseHTTPRequestHandler):
    """redirect_uri(http://localhost:8120/oauth)로 돌아온 인가코드 캡처."""

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}
        if parsed.path.startswith("/oauth"):
            _captured.update(params)
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            if "code" in params:
                msg = ("<h2 style='font-family:sans-serif'>카카오모먼트 인증 완료 ✅</h2>"
                       "<p style='font-family:sans-serif'>이 창을 닫고 터미널로 돌아가세요.</p>")
            else:
                msg = ("<h2 style='font-family:sans-serif'>인증 실패</h2>"
                       "<pre>%s</pre>" % json.dumps(params, ensure_ascii=False))
            self.wfile.write(msg.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *a):  # 콘솔 로그 억제
        pass


def _post_form(url, data):
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def _get(url, token):
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def _upload():
    """토큰·메타 파일 EC2로 scp."""
    if not KEY.exists():
        print(f"⚠️  SSH 키 없음({KEY}) — EC2 업로드 건너뜀. 로컬엔 저장됨.")
        return False
    ok = True
    for f in (TOKEN, META, APP):
        try:
            subprocess.run(
                ["scp", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=12",
                 "-i", str(KEY), str(f), f"{HOST}:{REMOTE_DATA}/{f.name}"],
                check=True, capture_output=True, timeout=60)
        except Exception as e:
            ok = False
            print(f"⚠️  업로드 실패({f.name}): {e}")
    if ok:
        print(f"✅ EC2 업로드 완료 → {HOST}:{REMOTE_DATA}")
    return ok


def main():
    DATA.mkdir(exist_ok=True)
    if not APP.exists():
        print(f"❌ {APP} 없음 — REST API 키 설정 파일이 필요합니다.")
        sys.exit(1)
    cfg = json.loads(APP.read_text())
    rest = (cfg.get("rest_api_key") or "").strip()
    secret = (cfg.get("client_secret") or "").strip()
    redirect = cfg.get("redirect_uri") or "http://localhost:8120/oauth"
    scope = cfg.get("scope") or "moment_management"
    if not rest:
        print("❌ kakao_app.json 의 rest_api_key 가 비어있습니다.")
        sys.exit(1)
    port = urllib.parse.urlparse(redirect).port or 8120

    # 로컬 콜백 서버 기동
    try:
        srv = HTTPServer(("127.0.0.1", port), _Handler)
    except OSError as e:
        print(f"❌ 포트 {port} 사용중({e}) — 다른 프로그램이 점유. 잠시 후 재시도하세요.")
        sys.exit(1)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    auth = AUTH_URL + "?" + urllib.parse.urlencode({
        "client_id": rest,
        "response_type": "code",
        "redirect_uri": redirect,
        "scope": scope,
    })
    print("브라우저에서 카카오 로그인 + 광고계정 동의를 진행하세요.")
    print("(광고주 카카오모먼트 광고계정에 접근권한 있는 계정으로 로그인)")
    print("\n창이 자동으로 안 뜨면 아래 주소를 복사해 여세요:\n")
    print(auth, "\n")
    webbrowser.open(auth)

    # 인가코드 대기 (최대 5분)
    deadline = time.time() + 300
    while time.time() < deadline and "code" not in _captured and "error" not in _captured:
        time.sleep(1)
    srv.shutdown()

    if "code" not in _captured:
        print("❌ 인가코드 못 받음:", _captured or "(5분 시간초과)")
        sys.exit(1)

    # 인가코드 → 비즈니스 토큰
    req = {"grant_type": "authorization_code", "client_id": rest,
           "redirect_uri": redirect, "code": _captured["code"]}
    if secret:
        req["client_secret"] = secret
    try:
        tok = _post_form(TOKEN_URL, req)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"❌ 토큰 발급 실패: {e.code} {body}")
        if "client_secret" in body.lower() or "KOE010" in body:
            print("\n→ 이 앱은 Client Secret 이 필수입니다.")
            print("  콘솔 [카카오 로그인]>[보안]에서 Client Secret 발급+'사용함' 설정 후")
            print("  data/kakao_app.json 의 client_secret 값에 넣고 다시 실행하세요.")
        sys.exit(1)

    TOKEN.write_text(json.dumps(tok, ensure_ascii=False, indent=2))
    rt_exp = tok.get("refresh_token_expires_in")
    valid_days = int(rt_exp // 86400) if rt_exp else 60
    META.write_text(json.dumps({
        "refreshed_at": datetime.date.today().isoformat(),
        "valid_days": valid_days,
    }, ensure_ascii=False, indent=2))
    print(f"\n✅ 비즈니스 토큰 저장 → {TOKEN.name}")
    print(f"   scope={tok.get('scope')}  refresh 유효 약 {valid_days}일")

    # 접근 가능한 광고계정 확인 (어떤 광고주를 수집할지 파악용)
    try:
        info = _get(TOKENINFO_URL, tok["access_token"])
        print("\n=== 토큰 정보 (접근 광고계정/scope) ===")
        print(json.dumps(info, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"(tokeninfo 조회 실패: {e} — 무시 가능)")

    _upload()
    print("\n끝났습니다. 이 창은 닫아도 됩니다.")


if __name__ == "__main__":
    main()
