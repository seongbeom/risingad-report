#!/usr/bin/env bash
# 더블클릭하면 네이버 광고주센터(ads.naver.com) 로그인 창이 뜨고, 로그인하면 세션 저장.
# 첫 로그인 시: 폰 2단계 인증 승인 + '이 브라우저는 2단계 인증 없이 로그인합니다' 체크.
cd "$(dirname "$0")"
./venv/bin/python3 naver_gfa_login.py
echo
echo "(엔터를 누르면 창이 닫힙니다)"
read -r _
