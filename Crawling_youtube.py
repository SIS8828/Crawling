import datetime
import json
import logging
import os
import re
import shutil
import traceback
import sys
import pandas as pd # 데이터를 처리하기 위한 가장 기본적인 패키지
import time # 사이트를 불러올 때, 작업 지연시간을 지정해주기 위한 패키지이다. (사이트가 늦게 켜지면 에러가 발생하기 때문)
import smtplib
from email.mime.text import MIMEText


from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver import Chrome
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait

# 파일을 읽어와 하나의 파일로 생성
def merge_excel(path,Youtube_info):
    exist_excel = pd.read_excel(path,
                                sheet_name = '시트명')
    results = []
    results.append(exist_excel)
    results.append(Youtube_info)
    all_data = pd.concat(results)
    all_data = all_data.drop_duplicates(['URL'])
    all_data.to_excel(path, index=False,
                            sheet_name = '시트명',
                            header=True,
                            startrow=0)

# 경로확인후 미존재시 생성
def check_path(data_path,result_path,stop_path):
    if not os.path.isdir(data_path):
        os.mkdir(data_path)
    if not os.path.isdir(result_path):
        os.mkdir(result_path)
    if not os.path.isdir(stop_path):
        os.mkdir(stop_path)

# xpath 존재 유무
def hasxpath(xpath,browser):
    try:
        browser.find_element_by_xpath(xpath)
        return True
    except:
        return False

# 이메일관련 함수
def mailing(bot):
    sendEmail = ""
    recvEmail = ""
    password = ""

    smtpName = "smtp.naver.com" #smtp 서버 주소
    smtpPort = 587 #smtp 포트 번호

    text = "크롤링종료"
    msg = MIMEText(text) #MIMEText(text , _charset = "utf8")

    msg['Subject'] = bot + "크롤링 성공결과"
    msg['From'] = sendEmail
    msg['To'] = recvEmail
    print(msg.as_string())

    s=smtplib.SMTP( smtpName , smtpPort ) #메일 서버 연결
    s.starttls() #TLS 보안 처리
    s.login( sendEmail , password ) #로그인
    s.sendmail( sendEmail, recvEmail, msg.as_string() ) #메일 전송, 문자열로 변환하여 보냅니다.
    s.close() #smtp 서버 연결을 종료합니다.

def main(argv):
    result = {'code': None}

    # 경로생성
    data_path = './data/' + time.strftime('%Y_%m', time.localtime(time.time()))
    result_path = './result/' + time.strftime('%Y_%m', time.localtime(time.time()))
    stop_path = './stop_data/' + time.strftime('%Y_%m', time.localtime(time.time()))
    
    # 수집된 URL이 이전에 수집된 데이터인지 확인하기 위한 엑셀
    excel_path = './excel/'
    excel_name = "채널정보가 담겨 있는 엑셀파일"
    
    fileName =  time.strftime('%Y_%m_%d', time.localtime(time.time())) + ".xlsx"
    fileName2 = "URL_" + time.strftime('%Y_%m_%d', time.localtime(time.time()))

    # 데이터를 담기위한 DataFrame
    Youtube_info = pd.DataFrame({'본부': [],
                                 '채널명': [],
                                 '동영상제목': [],
                                 'URL': [],
                                 '게시일': [],
                                 '조회수': [],
                                 '댓글 수': [],
                                 '좋아요수': [],
                                 '싫어요수': [],
                                 '수집일': [],
                                 '업데이트일': []})
    # 채널 정보를 담은 엑셀 가져오기
    ch = pd.read_excel(data_path + '/' + fileName2,
                            sheet_name = '경쟁사 콘텐츠 리스트'
                            )
    # 재실행시 URL체크용 List
    stop_ch_check_list = []
    # 이전에 가져온 URL 체크
    pre_ch_check_list = []
    # 성공시 저장을 위한 메세지
    message = "Success"

    if os.path.isfile(excel_path + '/' + excel_name):
        ch_check_pre = pd.read_excel(excel_path + '/' + excel_name,
                                sheet_name = '경쟁사 콘텐츠 리스트'
                                )
        pre_ch_check_list = ch_check_pre['URL'].tolist() # 이전에

    # 재실행 시 URL 체크로 이전에 가져왔던 데이터인지 아닌지 판단하기 위한 조건문
    if os.path.isfile(stop_path + '/' + fileName):
        ch_check_stop = pd.read_excel(stop_path + '/' + fileName,
                                sheet_name = '경쟁사 콘텐츠 리스트'
                                )
        stop_ch_check_list = ch_check_stop['URL'].tolist()

    ch_url = ch['URL'].tolist()  # 조회할 채널 URL
    # 재실행시 URL 필터
    sub_url = [x for x in ch_url if x not in stop_ch_check_list]
    # 첫실행시 기존 수집이력 존재 URL 제거
    sub_url = [x for x in sub_url if x not in pre_ch_check_list]

    ch = ch[ch['URL'].isin(sub_url)]

    ch_list = ch['채널명'].tolist() #조회할 채널명
    ch_list2 = ch['본부'].tolist() # 조회할 채널 그룹

    craw_date = ch['수집일'].tolist()
    z = 0

    delay = 2  # 3초지연
    cpt = webdriver.ChromeOptions()
    cpt.add_experimental_option('useAutomationExtension', False)  # chrome 실행시 나타나는 알람창 제거
    cpt.add_argument("lang=ko_KR")
    cpt.add_argument("--window-size=1920,1080")
    cpt.add_argument("--no-sandbox")
    cpt.add_argument("--disable-dev-shm-usage")
    browser = webdriver.Chrome(chrome_options=cpt, desired_capabilities=cpt.to_capabilities())
    browser.implicitly_wait(delay)
    # 경쟁사 채널 수 만큼 반복
    try:
        for i in sub_url:
            if i == "없음":
                z += 1
                continue

            if i in pre_ch_check_list:
                print( i + " || 수집된 이력이 존재하여 데이터수집하지 않습니다.")
                z += 1
                continue
            # 시작
            start_url = i # 채널이름 setting

            # 크롬으로 유튜브 조회시작
            browser.get(start_url)
            # 아직 검증하지 못함
            if hasxpath('//*[@id="rc-anchor-container"]',browser):
                print("/////////////////////reCAPTCHA 이슈발생 'Start' 입력필요//////////////////////////////")
                while True:
                    text = input("캡차를 처리 후 'Start'를 입력해주세요: ")
                    if text == "Start":
                        break
                body = browser.find_element_by_tag_name('body')
            if hasxpath('//*[@id="container"]/yt-player-error-message-renderer/yt-icon',browser):
                if browser.find_element_by_xpath('//*[@id="reason"]').text.split(" ")[0] == "비공개" or browser.find_element_by_xpath('//*[@id="subreason"]').text.split(" ")[1] == "삭제한":
                    print("영상이 비공개되어 모든 수치를 '없음' 처리합니다.")
                    insert_data = pd.DataFrame({'본부': [ch_list2[z]],
                                                 '채널명':[ch_list[z]],
                                                 '동영상제목': ["없음"],
                                                 'URL': [i],
                                                 '게시일': ["없음"],
                                                 '조회수': ["없음"],
                                                 '댓글 수': ["없음"],
                                                 '좋아요수': ["없음"],
                                                 '싫어요수': ["없음"],
                                                 '수집일': [craw_date[z]],
                                                 '업데이트일': [time.strftime('%Y-%m-%d',time.localtime(time.time()))]})
                    Youtube_info = Youtube_info.append(insert_data)
                    z += 1
                    continue

            # captcha
            if hasxpath('//*[@id="rc-anchor-container"]',browser):
                print("/////////////////////reCAPTCHA 이슈발생 'Start' 입력필요//////////////////////////////")
                while True:
                    text = input("캡차를 처리 후 'Start'를 입력해주세요: ")
                    if text == "Start":
                        break
                body = browser.find_element_by_tag_name('body')
            # 댓글 표시를 위한 pgdn
            body = browser.find_element_by_tag_name('body')
            body.send_keys(Keys.PAGE_DOWN)
            WebDriverWait(browser, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id = "comments"]'))
            )

            # 제목
            title = browser.find_element_by_xpath('//*[@id="container"]/h1/yt-formatted-string').text

            # 조회수
            try:
                view_count = \
                    browser.find_element_by_xpath('//*[@id="count"]/yt-view-count-renderer/span[1]').text.split(" ")[1][
                    :-1].replace(",", "")
                if view_count == "없":
                    view_count = 0
                elif view_count == "현재" or view_count == "대":
                    view_count = \
                        browser.find_element_by_xpath('//*[@id="count"]/yt-view-count-renderer/span[1]').text
                else:
                    view_count = int(view_count)
            except (NoSuchElementException, StaleElementReferenceException, TypeError, NameError, AttributeError) as e:
                print(e)
                print("조회수 정보 가져올수 없음")
                view_count = "없음"

            # 게시일
            try:
                date_check = browser.find_element_by_xpath('//*[@id="date"]/yt-formatted-string').text.split(" ")[0]
                if date_check == "2020.":
                    date = browser.find_element_by_xpath('//*[@id="date"]/yt-formatted-string').text[:-1].replace(" ","").replace('.','-')
                    date = datetime.datetime.strptime(date,'%Y-%m-%d')
                    date = date.isoformat()[:-9]
                elif date_check == "스트리밍" or date_check == "최초":
                    date = browser.find_element_by_xpath('//*[@id="date"]/yt-formatted-string').text
                else:
                    date = browser.find_element_by_xpath('//*[@id="date"]/yt-formatted-string').text
            except (NoSuchElementException, StaleElementReferenceException, TypeError, NameError, AttributeError) as e:
                print(e)
                print("날짜 정보 가져올수 없음")
                date = "없음"

            # 좋아요
            try:
                view_count2 = \
                    browser.find_element_by_xpath('//*[@id="count"]/yt-view-count-renderer/span[1]').text.split()[0]
                if view_count2 == "현재":
                    likes_check = browser.find_element_by_xpath(
                        '//*[@id="top-level-buttons"]/ytd-toggle-button-renderer[1]/a/yt-icon-button/button').get_attribute(
                        'aria-label').split(" ")[3][:-2]
                    likes_num = likes_check.replace(",", "")
                else:
                    likes_check = browser.find_element_by_xpath(
                        '//*[@id="top-level-buttons"]/ytd-toggle-button-renderer[1]/a/yt-formatted-string').get_attribute(
                        'aria-label').split()[1]
                    likes_num = likes_check[:-1].replace(",", "")
                if likes_num == "없":
                    likes_num = 0

                likes_num = int(likes_num)
            except (NoSuchElementException, StaleElementReferenceException, TypeError, NameError, AttributeError) as e:
                print(e)
                print("좋아요 정보를 가져올 수 없음")
                likes_num = "없음"
            # 싫어요
            try:
                view_count2 = \
                    browser.find_element_by_xpath('//*[@id="count"]/yt-view-count-renderer/span[1]').text.split()[0]
                if view_count2 == "현재":
                    dislikes_check = browser.find_element_by_xpath(
                        '//*[@id="top-level-buttons"]/ytd-toggle-button-renderer[2]'
                        '/a/yt-icon-button/button').get_attribute('aria-label').split(" ")[3][:-2]
                    dislikes_num = dislikes_check.replace(",", "")
                else:
                    dislikes_check = \
                    browser.find_element_by_xpath('//*[@id="top-level-buttons"]/ytd-toggle-button-renderer[2]'
                                                  '/a/yt-formatted-string').get_attribute(
                        'aria-label').split()[1]
                    dislikes_num = dislikes_check[:-1].replace(",", "")
                if dislikes_num == "없":
                    dislikes_num = 0

                dislikes_num = int(dislikes_num)
            except (NoSuchElementException, StaleElementReferenceException, TypeError, NameError, AttributeError) as e:
                print(e)
                print("싫어요 정보를 가져올 수 없음")
                dislikes_num = "없음"

            # 댓글수
            try:
                date_check = browser.find_element_by_xpath('//*[@id="date"]/yt-formatted-string').text.split()[0]
                if date_check != "스트리밍":
                    comment = browser.find_element_by_xpath('//*[@id = "header"]/ytd-comments-header-renderer'
                                                            '/div[1]/h2/yt-formatted-string').text.split()[1][
                              :-1].replace(",", "")
                    comment = int(comment)
                else:
                    comment = "없음"
            except (
            NoSuchElementException, StaleElementReferenceException, TypeError, NameError, AttributeError) as e:
                print(e)
                comment = "댓글이 사용 중지되었습니다."
                print("댓글사용이 중지되어 comment 값을 '중지'로 처리합니다. ")
            # 크롤링한 데이터를 담는다.
            insert_data = pd.DataFrame({'본부': [ch_list2[z]],
                                         '채널명':[ch_list[z]],
                                         '동영상제목': [title],
                                         'URL': [i],
                                         '게시일': [date],
                                         '조회수': [view_count],
                                         '댓글 수': [comment],
                                         '좋아요수': [likes_num],
                                         '싫어요수': [dislikes_num],
                                         '수집일': [craw_date[z]],
                                         '업데이트일': [time.strftime('%Y-%m-%d',time.localtime(time.time()))]})
            Youtube_info = Youtube_info.append(insert_data)
            print("end: " + str(datetime.datetime.now()))
            z += 1
            print('////////////////////////////////////////')
    except Exception as e:
        print(e)
        print("아직 정의되지 않은 에러 처리필요")
        # 중간에 실패시 메세지를 Fail 처리하여 중간데이터를 stop_data로 처리한다.
        message = "Fail"
        result['code'] = 'fail'
        check_path(data_path,result_path,stop_path)
        if os.path.isfile(stop_path + '/' + fileName):
            print("기존 파일이 존재하여 병합 후 저장합니다.")
            merge_excel(stop_path + '/' + fileName,Youtube_info)
        else:
            Youtube_info.to_excel(stop_path + '/' + fileName,
                                  index=False,
                                  sheet_name='경쟁사 콘텐츠 리스트',
                                  header=True,
                                  startrow=0)
        print(datetime.datetime.now())
        print(message)
    finally:
        if message == "Success":
            check_path(data_path,result_path,stop_path)
            print("=================금일 업데이트된 유튜브 정보 수집완료=======================")
            print("================= 데이터 산출중입니다 ========================")
            result['code'] = 'success'
            # stopdata 파일존재시 병합
            if os.path.isfile(stop_path + '/' + fileName):
                print("기존 파일이 존재하여 병합 후 저장합니다.")
                merge_excel(stop_path + '/' + fileName,Youtube_info)
                shutil.copyfile(os.path.join(stop_path + '/', fileName), os.path.join(data_path + '/', fileName))
            else:
                Youtube_info.to_excel(data_path + '/' + fileName,
                                      index=False,
                                      sheet_name='경쟁사 콘텐츠 리스트',
                                      header=True,
                                      startrow=0)
            # 작업완료일경우 txt파일로 남겨줌
            shutil.copyfile(os.path.join('./','bot.txt'),os.path.join(data_path,fileName+'_Success.txt'))
            print(datetime.datetime.now())
            print(message)
            mailing("bot1")

        print('finish')
        json_str = json.dumps(result, ensure_ascii=False)
        print("<peon>")
        print(json_str)
        print("</peon>")

        print(datetime.datetime.now())


if __name__ == "__main__":
    main(sys.argv)
