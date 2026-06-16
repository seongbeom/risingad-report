#!/usr/bin/env bash
# 더블클릭하면 '세션 갱신 도우미'를 맥 백그라운드에 설치합니다 (launchd, 10분마다).
# 설치 후: 웹 관리화면에서 '🔄 갱신 요청'을 누르면 이 맥이 감지해 로그인 창을 자동으로 띄우고,
#          세션 만료 임박 시 맥 알림을 띄웁니다. (재부팅해도 유지)
set -e
cd "$(dirname "$0")"
REPO="$(pwd)"
LABEL="com.cafe24.sessionguard"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
PY="$(command -v python3 || echo /usr/bin/python3)"

echo "▶ repo: $REPO"

# 1) GUARD_TOKEN 보장 (.env). 없으면 생성.
if ! grep -q '^GUARD_TOKEN=' .env 2>/dev/null; then
  TOK="$(python3 -c 'import secrets;print(secrets.token_hex(16))')"
  echo "GUARD_TOKEN=$TOK" >> .env
  echo "✅ .env 에 GUARD_TOKEN 생성: $TOK"
  echo "   ⚠️ 같은 값을 EC2 의 /opt/cafe24/.env 에도 넣고 서비스 재시작해야 합니다."
  echo "      (관리자에게: GUARD_TOKEN=$TOK)"
else
  echo "✅ GUARD_TOKEN 이미 설정됨(.env)"
fi
grep -q '^GUARD_SERVER_URL=' .env 2>/dev/null || echo "GUARD_SERVER_URL=http://52.79.112.252:9090" >> .env

# 2) launchd plist 작성
mkdir -p "$HOME/Library/LaunchAgents" data
cat > "$PLIST" <<PLISTEOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$PY</string>
    <string>$REPO/session_guard.py</string>
  </array>
  <key>WorkingDirectory</key><string>$REPO</string>
  <key>StartInterval</key><integer>600</integer>
  <key>RunAtLoad</key><true/>
  <key>StandardOutPath</key><string>$REPO/data/guard.log</string>
  <key>StandardErrorPath</key><string>$REPO/data/guard.log</string>
</dict>
</plist>
PLISTEOF
echo "✅ plist 작성: $PLIST"

# 3) (재)로드
launchctl unload "$PLIST" 2>/dev/null || true
launchctl load -w "$PLIST"
echo "✅ 백그라운드 도우미 가동 시작 (10분마다 점검, 재부팅해도 유지)"
echo
echo "테스트: 지금 한 번 실행해봅니다 ↓"
"$PY" "$REPO/session_guard.py" || true
echo
echo "끝났습니다. 이 창은 닫아도 됩니다. (제거하려면 세션도우미_제거.command)"
read -r _
