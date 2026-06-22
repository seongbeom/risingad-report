#!/usr/bin/env bash
# 배포 스크립트 — git push 후 서버에서 pull + restart 까지 한 번에.
# 사용법: ./deploy.sh
#
# 의도: 코드 변경마다 service restart 하면 APScheduler 잡 타이밍이 리셋되거나
# 진행 중인 스크래핑이 끊김. 그래서 commit/push 를 묶어 1회만 restart.

set -e

KEY=~/cafe24_migration/cafe24-new-key.pem
SERVER_HOST=52.79.112.252
HOST=ubuntu@$SERVER_HOST
REMOTE_DIR=/opt/cafe24
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=12"

# 1) 로컬 dirty 체크
if [ -n "$(git status --porcelain)" ]; then
  echo "❌ 커밋 안 된 변경사항이 있습니다. 먼저 commit 하세요."
  git status --short
  exit 1
fi

# 1.5) 이번 배포가 건드리는 파일 판별 — .py(또는 requirements) 변경 없으면 '템플릿 전용'
#      → 리로드 스킵(앱이 TEMPLATES_AUTO_RELOAD 로 즉시 반영). 라이브 스크래퍼 무중단.
CHANGED=$(git diff --name-only origin/master HEAD 2>/dev/null)
NEEDS_RELOAD=0
[ -z "$CHANGED" ] && NEEDS_RELOAD=1   # 변경목록 못 구하면 안전하게 리로드
for f in $CHANGED; do
  case "$f" in
    *.py|requirements*.txt) NEEDS_RELOAD=1 ;;
  esac
done
echo "▶ 변경 파일:"; echo "$CHANGED" | sed 's/^/    /'
[ "$NEEDS_RELOAD" = "1" ] && echo "  → 코드(.py) 변경 있음: graceful 리로드 진행" \
                          || echo "  → 템플릿/문서 전용: 리로드 스킵(스크래퍼 무중단)"

# 2) push
echo "▶ git push origin master"
git push origin master

# 3) 서버에서 pull + graceful reload (systemctl restart 안 함 → 진행 중인 스크랩 안 죽임)
#    배포 코드를 새로 올리되, 라이브 스크랩은 계정 경계/idle 에서 스스로 os.execv 로 재적재.
echo "▶ 배포 전 프로세스 시작시각 기록"
BEFORE=$(curl -s --max-time 8 "http://$SERVER_HOST:9090/healthz" 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('started_at',''))" 2>/dev/null || echo "")
echo "  현재 started_at=$BEFORE"

# 템플릿/문서 전용 배포 → 서버 git pull 만 하고 리로드 없이 종료 (스크래퍼 안 건드림)
if [ "$NEEDS_RELOAD" != "1" ]; then
  echo "▶ 서버 git pull (리로드 없음 — 템플릿 즉시 반영)"
  ssh $SSH_OPTS -i "$KEY" "$HOST" "cd $REMOTE_DIR && git pull --ff-only"
  echo "✅ 배포 완료 (템플릿 전용 — 스크래퍼 무중단, 새로고침하면 반영됨)"
  exit 0
fi

echo "▶ 서버 git pull + 문법검사 + reload 요청"
ssh $SSH_OPTS -i "$KEY" "$HOST" "cd $REMOTE_DIR && git pull --ff-only && \
  ./venv/bin/python3 -c 'import ast;ast.parse(open(\"app.py\").read())' && \
  curl -s -X POST http://127.0.0.1:9090/admin/request_reload || { echo '문법오류 or reload요청 실패 — 배포 중단'; exit 1; }"

# 4) reload 완료 대기 — started_at 이 바뀌면 새 코드로 재적재 성공. 최대 RELOAD_MAX 초.
RELOAD_MAX=${RELOAD_MAX:-360}
echo "▶ graceful reload 대기 (최대 ${RELOAD_MAX}s — 스크랩 중이면 계정 끝나고 리로드)"
waited=0; ok=0
while [ "$waited" -lt "$RELOAD_MAX" ]; do
  NOW=$(curl -s --max-time 8 "http://$SERVER_HOST:9090/healthz" 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('started_at',''))" 2>/dev/null || echo "")
  if [ -n "$NOW" ] && [ "$NOW" != "$BEFORE" ]; then echo "  ✅ 새 코드 적재됨 (started_at=$NOW)"; ok=1; break; fi
  sleep 10; waited=$((waited+10)); echo "  대기중... (${waited}/${RELOAD_MAX}s)"
done

# 5) 그래도 안 바뀌면(프로세스 wedged) 최후수단 systemctl restart.
#    단, 스크랩 진행 중이면 절대 안 끊음 — idle 빈틈까지 기다렸다 restart (graceful 실패 시 안전망).
if [ "$ok" != "1" ]; then
  echo "⚠️ reload 미확인 — idle 대기 후 최후수단 systemctl restart (스크랩 중이면 끊지 않음)"
  fb=0
  while [ "$fb" -lt 180 ]; do
    run=$(curl -s --max-time 8 "http://$SERVER_HOST:9090/healthz" 2>/dev/null | python3 -c "import sys,json;print(len(json.load(sys.stdin).get('running_now',[])))" 2>/dev/null || echo "?")
    # 대기 중 graceful 이 뒤늦게 적재됐을 수도 → started_at 재확인
    NOW=$(curl -s --max-time 8 "http://$SERVER_HOST:9090/healthz" 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('started_at',''))" 2>/dev/null || echo "")
    if [ -n "$NOW" ] && [ "$NOW" != "$BEFORE" ]; then echo "  ✅ 뒤늦게 graceful 적재 확인 — restart 불필요"; ok=1; break; fi
    if [ "$run" = "0" ]; then
      echo "  idle 확인 → systemctl restart"
      ssh $SSH_OPTS -i "$KEY" "$HOST" "sudo systemctl restart cafe24 && sleep 3 && sudo systemctl is-active cafe24"
      ok=1; break
    fi
    echo "  스크랩 중(running=$run) — 끊지 않고 대기 (${fb}/180s)"; sleep 15; fb=$((fb+15))
  done
fi

# 6) healthcheck
echo "▶ /healthz 확인"
ssh $SSH_OPTS -i "$KEY" "$HOST" "curl -s http://127.0.0.1:9090/healthz | python3 -m json.tool || echo 'healthz 응답 없음'"

echo "✅ 배포 완료"
