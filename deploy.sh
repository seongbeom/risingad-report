#!/usr/bin/env bash
# 배포 스크립트 — git push 후 서버에서 pull + restart 까지 한 번에.
# 사용법: ./deploy.sh
#
# 의도: 코드 변경마다 service restart 하면 APScheduler 잡 타이밍이 리셋되거나
# 진행 중인 스크래핑이 끊김. 그래서 commit/push 를 묶어 1회만 restart.

set -e

KEY=~/.ssh/cafe24-key.pem
HOST=ubuntu@13.209.254.190
REMOTE_DIR=/opt/cafe24

# 1) 로컬 dirty 체크
if [ -n "$(git status --porcelain)" ]; then
  echo "❌ 커밋 안 된 변경사항이 있습니다. 먼저 commit 하세요."
  git status --short
  exit 1
fi

# 2) push
echo "▶ git push origin master"
git push origin master

# 3) 서버에서 pull + restart
echo "▶ 서버 배포 (pull + restart)"
ssh -i "$KEY" "$HOST" "cd $REMOTE_DIR && git pull --ff-only && sudo systemctl restart cafe24 && sleep 3 && sudo systemctl is-active cafe24"

# 4) healthcheck
echo "▶ /healthz 확인"
ssh -i "$KEY" "$HOST" "curl -s http://127.0.0.1:9090/healthz | python3 -m json.tool || echo 'healthz 응답 없음'"

echo "✅ 배포 완료"
