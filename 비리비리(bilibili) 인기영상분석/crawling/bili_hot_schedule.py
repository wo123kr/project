from pprint import pprint
import requests
import pandas as pd
import time
import random
import datetime
import time
from sqlalchemy import create_engine

# 경고 메시지를 무시하기 위한 라이브러리
import warnings                          
warnings.filterwarnings("ignore")

# 스케줄 관련 라이브러리
import schedule
import time

aws_postgresql_url = 'postgresql://alpha:aA!adoba2018@adobadb-kr-1.c1wymupg5wtq.ap-northeast-2.rds.amazonaws.com:5432/adoba_kr'
engine_postgresql = create_engine(aws_postgresql_url)

def job():
    print("작업 중...")
    print(datetime.datetime.now())

    def tp_to_datetime(tp):
        from datetime import datetime
        tp = datetime.fromtimestamp(tp).strftime('%Y-%m-%d %H:%M:%S')
        return tp

    # 데이터에 삽입할 날짜와 시간 생성
    now = datetime.datetime.now()
    nowDatetime = now.strftime('%Y-%m-%d %H:%M:%S')

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
        publish_at = [] #发布日期 발행일자
        #pubdate = [] #上转日期 업로드일자
        #ctime = []  #创建日期 생성일
        url = i[1] # url地址 
        tab_name = i[0] # tab名称  인기영상 카테고리
        title_list = [] # 标题列表 #영상제목
        play_cnt_list = [] # 播放量 조회수
        danmu_cnt_list = [] #弹幕量 탄막수
        coin_cnt_list = [] #硬币量 토큰수
        like_cnt_list = [] #点赞量 좋아요수
        #dislike_cnt_list = [] #点踩量 싫어요수
        share_cnt_list = [] #分享量 공유수
        favorite_cnt_list = [] #收藏量 즐겨찾기수
        reply_cnt_list = [] #回复量 댓글수
        author_list = [] # 作者 작성자
        mid_list = [] # 作者id 작성자id
        #score_list = [] #종합점수
        video_url = [] #영상주소
        bvid_list = [] #bvid
        regdate = nowDatetime   #데이터 저장일
        tname = [] #카테고리
        pub_location = [] #발행지역
        
        try:
            r = requests.get(url, headers=headers)
            # print(r.status_code)
            # pprint(r.content.decode())
            # pprint(r.json())
            json_data = r.json()
            list_data = json_data['data']['list']
            rank_no = 0
            
            for data in list_data:
                rank_no += 1       
                rank.append(rank_no) #排名
                his_rank.append(data['stat']['his_rank']) #排名 
                #pubdate.append(data['pubdate']) #上转日期
                #ctime.append(data['ctime']) #创建日期
                publish_at.append(tp_to_datetime(data['pubdate'])) 
                title_list.append(data['title'])  # 视频标题 영상제목
                play_cnt_list.append(data['stat']['view'])  # 播放数 조회수
                danmu_cnt_list.append(data['stat']['danmaku'])  # 弹幕数 탄막수
                coin_cnt_list.append(data['stat']['coin'])  # 投币数 토큰수
                like_cnt_list.append(data['stat']['like'])  # 点赞数 좋아요수
                #dislike_cnt_list.append(data['stat']['dislike'])  # 点踩数 싫어요수
                share_cnt_list.append(data['stat']['share'])  # 分享数 공유수
                favorite_cnt_list.append(data['stat']['favorite'])  # 收藏数 즐겨찾기수
                reply_cnt_list.append(data['stat']['reply'])  # 回复数 댓글수
                author_list.append(data['owner']['name'])  # 作者 작성자
                mid_list.append(data['owner']['mid'])  # 作者id 작성자id
                #score_list.append(data['score'])  # 评分 점수
                video_url.append('https://www.bilibili.com/video/' + data['bvid'])
                bvid_list.append(data['bvid'])
                try :
                    tname.append(data['tname']) #카테고리
                except: 
                    tname.append('')
                try :
                    pub_location.append(data['pub_location']) #발행지역
                except:
                    pub_location.append('')
                
        except Exception as e:
            print("크롤링실패 :{}".format(str(e))) # 크롤링 실패시 에러메시지 출력
        
        time.sleep(random.uniform(2,4)) # 로딩시간 랜덤시간으로 대체
        
        items = pd.DataFrame(
                {'rank_no' : rank,
                'his_rank': his_rank,
                'publish_at': publish_at,
                #'pubdate': pubdate,
                #'ctime': ctime,
                'title': title_list,  
                'url': video_url,
                'bvid': bvid_list,
                'author': author_list,
                'mid': mid_list,   
                #'score_count': score_list,
                'view_count': play_cnt_list,
                'damu_count': danmu_cnt_list,
                'coin_count': coin_cnt_list,
                'like_count': like_cnt_list,
                #'dslike_count': dislike_cnt_list,
                'share_count': share_cnt_list,
                'favorite': favorite_cnt_list,
                'reply_count': reply_cnt_list,
                'tab_name': tab_name,
                'regdate' : regdate,
                'tname' : tname,
                'pub_location' : pub_location
                })
        
        items.to_sql(name='bili_top100', con=engine_postgresql, if_exists='append',index=False)  
        #print(items)


# 매일 오전 9시 20분에 코드를 실행
schedule.every().day.at("09:20").do(job)

while True:
    #무한 루프를 돌면서 스케쥴을 유지한다.
    schedule.run_pending()
    time.sleep(1)
