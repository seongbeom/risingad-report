#!/usr/bin/env bash
# 더블클릭하면 카카오 동의화면이 뜨고, 로그인+광고계정 동의하면 비즈니스 토큰 발급 + EC2 업로드까지 자동.
# 월 1회만 하면 됨.
cd "$(dirname "$0")"
./venv/bin/python3 kakao_login.py
echo
echo "(엔터를 누르면 창이 닫힙니다)"
read -r _
