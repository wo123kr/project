from pprint import pprint
import requests
import pandas as pd
import time
import random
import datetime
import time
import pymysql
from sqlalchemy import create_engine

db_connection_str = 'mysql+pymysql://jinwoo:1fAwVji21!3ifjwo@rm-m5e1961o8lqvj3664po.mysql.rds.aliyuncs.com/adoba_data'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()  

# def createFolder(directory):
#     try:
#         if not os.path.exists(directory):
#             os.makedirs(directory)
#     except OSError:
#         print ('Error: Creating directory. ' + directory)
        
#     createFolder("D:/OneDrive - 아도바/adoba data/Tableau_data/인기영상데이터/B站TOP100/" + "filename")

def tp_to_date(tp): #把tp格式的时间转换为日期格式
    from datetime import datetime
    """
    把tp格式的时间转换为日期格式
    :param tp:
    :return:
    """
    tp = datetime.fromtimestamp(tp).strftime( '%Y-%m-%d') 
    return tp

def tp_to_time(tp):
    from datetime import datetime
    """
    把tp格式的时间转换为日期格式
    :param tp:
    :return:
    """
    tp = datetime.fromtimestamp(tp).strftime('%H:%M:%S')
    return tp


def bilibili():
    # 데이터에 삽입할 날짜와 시간 생성
    now = datetime.datetime.now()
    nowDatetime = now.strftime('%Y-%m-%d %H:%M:%S')
    filename = datetime.datetime.now().strftime("%Y-%m-%d") 

    url_dict = {
        '全站': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=0&type=all', #전체 랭킹
        # '番剧': 'https://api.bilibili.com/pgc/web/rank/list?day=3&season_type=1',  #
        # '国产动画': 'https://api.bilibili.com/pgc/season/rank/web/list?day=3&season_type=4',  #
        # '国创相关': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=168&type=all',  #
        # '纪录片': 'https://api.bilibili.com/pgc/season/rank/web/list?day=3&season_type=3',  #
        '动画': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=1&type=all',
        '音乐': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=3&type=all',
        '舞蹈': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=129&type=all',
        '游戏': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=4&type=all',
        '知识': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=36&type=all',
        '科技': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=188&type=all',
        '运动': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=234&type=all', 
        '汽车': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=223&type=all',
        '生活': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=160&type=all',
        '美食': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=211&type=all',
        '动物圈': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=217&type=all',
        '鬼畜': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=119&type=all',
        '时尚': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=155&type=all',
        '娱乐': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=5&type=all',
        '影视': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=181&type=all',
        # # '电影': 'https://api.bilibili.com/pgc/season/rank/web/list?day=3&season_type=2',  #
        # # '电视剧': 'https://api.bilibili.com/pgc/season/rank/web/list?day=3&season_type=5',  #
        '原创': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=0&type=origin',
        '新人': 'https://api.bilibili.com/x/web-interface/ranking/v2?rid=0&type=rookie',
    }

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Origin': 'https://www.bilibili.com',
        'Accept-Encoding': 'gzip, deflate, br',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
        'Accept-Language': 'zh-cn',
        'Connection' : 'keep-alive',
        'Referer': 'https://www.bilibili.com/v/popular/rank/all'
    }

    for i in url_dict.items():
        rank = []  #排名 순위
        his_rank = [] #排名 이전순위
        pubdate = [] #上转日期 업로드일자
        ctime = []  #创建日期 생성일
        url = i[1] # url地址 
        tab_name = i[0] # tab名称  인기영상 카테고리
        title_list = [] # 标题列表 #영상제목
        play_cnt_list = [] # 播放量 조회수
        danmu_cnt_list = [] #弹幕量 탄막수
        coin_cnt_list = [] #硬币量 토큰수
        like_cnt_list = [] #点赞量 좋아요수
        dislike_cnt_list = [] #点踩量 싫어요수
        share_cnt_list = [] #分享量 공유수
        favorite_cnt_list = [] #收藏量 즐겨찾기수
        reply_cnt_list = [] #回复量 댓글수
        author_list = [] # 作者 작성자
        score_list = [] #종합점수
        video_url = [] #영상주소
        regdate = nowDatetime   #데이터 저장일
        
        try:
            r = requests.get(url, headers=headers)
            print(r.status_code)
            pprint(r.content.decode())
            pprint(r.json())
            json_data = r.json()
            list_data = json_data['data']['list']
            rank_no = 0
            
            for data in list_data:
                rank_no += 1
                rank.append(rank_no) #排名
                his_rank.append(data['stat']['his_rank']) #排名 
                pubdate.append(tp_to_date(data['pubdate'])) #发布日期 발행일
                ctime.append(tp_to_time(data['ctime'])) #创建日期 생성일
                title_list.append(data['title'])  # 视频标题 영상제목
                play_cnt_list.append(data['stat']['view'])  # 播放数 조회수
                danmu_cnt_list.append(data['stat']['danmaku'])  # 弹幕数 탄막수
                coin_cnt_list.append(data['stat']['coin'])  # 投币数 토큰수
                like_cnt_list.append(data['stat']['like'])  # 点赞数 좋아요수
                dislike_cnt_list.append(data['stat']['dislike'])  # 点踩数 싫어요수
                share_cnt_list.append(data['stat']['share'])  # 分享数 공유수
                favorite_cnt_list.append(data['stat']['favorite'])  # 收藏数 즐겨찾기수
                reply_cnt_list.append(data['stat']['reply'])  # 回复数 댓글수
                author_list.append(data['owner']['name'])  # 作者 작성자
                score_list.append(data['score'])  # 评分 점수
                video_url.append('https://www.bilibili.com/video/' + data['bvid'])  
                
        except Exception as e:
            print("크롤링실패 :{}".format(str(e))) # 크롤링 실패시 에러메시지 출력
        
        time.sleep(random.uniform(2,4)) # 로딩시간 랜덤시간으로 대체
    
        items = pd.DataFrame(
                {'rank_no' : rank,
                'his_rank': his_rank,
                'pubdate': pubdate,
                'ctime': ctime,
                'title': title_list,  
                'url': video_url,
                'author': author_list,    
                'score_count': score_list,
                'view_count': play_cnt_list,
                'damu_count': danmu_cnt_list,
                'coin_count': coin_cnt_list,
                'like_count': like_cnt_list,
                'dslike_count': dislike_cnt_list,
                'share_count': share_cnt_list,
                'favorite': favorite_cnt_list,
                'reply_count': reply_cnt_list,
                'tab_name':tab_name,
                'regdate' : regdate
                })
        
#         items.to_excel(path + '['+ filename+ ']' + 'B站TOP100-{}.xlsx'.format(tab_name), encoding='utf-8', index=False, engine='xlsxwriter') # csv 파일로 저장
#         print("===========================================================")
#         print('写入成功：' + 'B站TOP100-{}.csv'.format(tab_name)) # 写入成功시 출력
        print('{} 크롤링 완료'.format(tab_name))
        items.to_sql(name='bili_top100', con=db_connection, if_exists='append',index=False)  
        print('{} 저장 완료'.format(tab_name))

bilibili()