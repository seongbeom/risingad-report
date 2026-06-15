#!/usr/bin/env bash
# 더블클릭하면 크리테오 로그인 창이 뜨고, 로그인하면 세션 갱신 + EC2 업로드까지 자동.
# 월 1회만 하면 됨.
cd "$(dirname "$0")"
./venv/bin/python3 criteo_login.py
echo
echo "(엔터를 누르면 창이 닫힙니다)"
read -r _
