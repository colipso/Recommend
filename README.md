Recommend
====== 

安装说明
====== 
* python2.7.13 
* spark 2.0.1 
* 安装目录下创建文件夹 ./data ./log 


使用说明
====== 
* 启动新闻数据存储服务 python GetAndSaveNews.py 
* 启动用户行为存储服务 python GetAndSaceActions.py 
* 启动推荐引擎 python RecommendServer.py 
* 传入新闻数据 
    * post 
    * url：http://***:8888/sendnews
    * 写入数据：{title:***,tag:****, publishTime:***, from:***, content:**** , pv:*** , id:*****}
    * 注意：pv不能省略
* 传入用户行为 
    * post 
    * url：http://***:8887/sendactions
    * 写入数据：{userId:***,newsId:****,rating:***} 
    * 注意：rating是用户对新闻兴趣度,范围0-10 。0是有推送无浏览。数值越大越兴趣越高。 
* 获得推荐
    * get 
    * url：http://127.0.0.1:8886/getrecommend?userid=276729&tag=娱乐,探索
    * 返回推荐文章id列表：{'recommend':[1,2,3,4]}
    * 注意：userid和tag都不能省略，tag用于根据新闻标签推荐。 

算法 
======  
* 协同过滤 
* 根据新闻标签，结合新闻热度推荐。 




