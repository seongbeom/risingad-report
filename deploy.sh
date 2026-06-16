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

# 2) push
echo "▶ git push origin master"
git push origin master

# 3) 라이브 스크랩이 계정 처리 중이면 빈틈까지 잠깐 대기 (계정 도중 kill → 좀비 chromium/EPIPE 방지)
#    최대 GRACE_MAX 초 대기, 그 안에 idle 못 잡으면 그냥 진행.
GRACE_MAX=${GRACE_MAX:-150}
echo "▶ 라이브 idle 대기 (최대 ${GRACE_MAX}s — 계정 처리 중이면 빈틈까지)"
waited=0
while [ "$waited" -lt "$GRACE_MAX" ]; do
  running=$(curl -s --max-time 8 "http://$SERVER_HOST:9090/healthz" 2>/dev/null | python3 -c "import sys,json;print(len(json.load(sys.stdin).get('running_now',[])))" 2>/dev/null || echo "?")
  if [ "$running" = "0" ]; then echo "  idle 확인 — 재시작 진행"; break; fi
  echo "  스크랩 진행 중(running=$running) — 5s 대기 (${waited}/${GRACE_MAX}s)"
  sleep 5; waited=$((waited+5))
done

# 4) 서버에서 pull + restart
echo "▶ 서버 배포 (pull + restart)"
ssh $SSH_OPTS -i "$KEY" "$HOST" "cd $REMOTE_DIR && git pull --ff-only && sudo systemctl restart cafe24 && sleep 3 && sudo systemctl is-active cafe24"

# 4) healthcheck
echo "▶ /healthz 확인"
ssh $SSH_OPTS -i "$KEY" "$HOST" "curl -s http://127.0.0.1:9090/healthz | python3 -m json.tool || echo 'healthz 응답 없음'"

echo "✅ 배포 완료"
