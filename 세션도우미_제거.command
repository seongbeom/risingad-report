#!/usr/bin/env bash
# 더블클릭하면 세션 갱신 도우미(백그라운드)를 제거합니다.
cd "$(dirname "$0")"
LABEL="com.cafe24.sessionguard"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
launchctl unload "$PLIST" 2>/dev/null || true
rm -f "$PLIST"
echo "✅ 세션 갱신 도우미 제거됨 (.env 의 GUARD_TOKEN 은 유지 — 수동 삭제 가능)"
echo "끝났습니다. 이 창은 닫아도 됩니다."
read -r _
