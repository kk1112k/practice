## 셋팅 방법
```
## 필수 설치 목록 ##
python 3.7.16
nodejs
```

-- sqlite 문제시 해결방안 
export LD_LIBRARY_PATH="/usr/local/lib"

1. 가상환경
    - python -m venv .venv
    - windows : .\.venv\Scripts\activate
    - linux : source .venv/bin/activate
    - python -m pip install --upgrade pip
    - pip install -r requirements.txt

2. gulp 설치
    - npm install --global gulp-cli

3. gulp 의존성 패키지 목록 설치
    - npm install --save-dev gulp gulp-util
    > 모듈을 설치(npm install)할 때 --save-dev 명령어를 입력하면 개발할 때만 의존(devDependcies)하는 모듈로 기술
    > 보통 개발 당시에만 gulp의 플러그인 패키지가 필요하고 배포(Production)시에는 gulp 패키지 모듈은 서비스를 받는 사용자에게 필요없기 때문에 개발할 때만 의존하도록 --save-dev 플래그를 사용
    > 만약에 $ npm install gulp --save와 같이 -dev를 생략하고 명령어를 입력하면 배포할 때도 의존하는 모듈로 기술

4. gulp 명령어
    - gulp build # 파일 떨굼 # python manage.py runserver 만으로 실행 가능
    - gulp default # 파일 없이 디자인만 씌움 # python manage.py runserver, glup default 2개 콘솔 띄워야함

4. user 생성
    - python manage.py createsuperuser

5. smtp 설정
    - ACCOUNT_EMAIL_VERIFICATION = "none" #none, optional(default, 로그인 알림 메일 발송), mandatory(이메일인증받지않으면 로그인할수없음)

```
라이브러리 관련
pip freeze > requirements.txt
pip install -r requirements.txt

# 참고
pip install django django-allauth django-embed-video django-crispy-forms social-auth-app-django django_drf_filepond
```

소스 업데이트
cd /home/ec2-user/environment/dap
git reset --hard
git pull origin master

git fetch --all

실행 방법
export LD_LIBRARY_PATH="/usr/local/lib"
cd /home/ec2-user/environment/dap
source .venv/bin/activate
curl http://checkip.amazonaws.com
python manage.py runserver 0.0.0.0:8080


git push ssh://git-codecommit.ap-northeast-2.amazonaws.com/v1/repos/DAP-dev# practice
