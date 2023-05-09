import json
from tqdm import tqdm
import jieba
import time
import csv
from multiprocessing import Process,Manager
import psutil
from matplotlib import pyplot as plt

def ReadData(file_path):
    '''
    读取json文件,返回行的列表
    '''
    result=[]
    with open(file_path,encoding='utf8') as f:
        data=json.load(f)
        for line in tqdm(data,desc='reading...'):
            result.append(line.get('content'))
    return result
def LoadStopwords(dict_path):
    '''
    jieba载入分词库,返回分词集合
    '''
    jieba.load_userdict(dict_path)
    f=open(dict_path,'r',encoding='utf8')
    stop_words={line.strip() for line in f.readlines()}
    f.close()
    return stop_words
def Map(data,stop_words,result):
    '''
    Map进程读取文档并进行词频统计，返回该文本的词频统计结果
    '''
    count_dict={}
    for line in data:
        words=jieba.lcut(line)
        for word in words:
            if word in stop_words:
                continue
            elif word not in count_dict:
                count_dict.update({word:0})
            count_dict[word]+=1
    result.append(count_dict)
def Reduce(result_lis,save_path):
    '''
    整合所有结果
    '''
    result_all={}
    for result in tqdm(result_lis,desc='reducing...'):
        for key,value in result.items():
            if key not in result_all:
                result_all.update({key:0})
            result_all[key]+=value
    with open('test.csv','w',newline='',encoding='utf8') as f:
        writer=csv.writer(f)
        for row in result_all.items():
            writer.writerow(row)
if __name__=='__main__':
    file_path='D:/Project/Python/week11MapReduce/sohu_data.json'
    dict_path='D:/Project/Python/week11MapReduce/stopwords_list.txt'
    save_path='D:/Project/Python/week11MapReduce/result.csv'
    #N=[psutil.cpu_count(False)]
    N=[1,2,3,4,5,6,7,8,9,10,11,12]
    data=ReadData(file_path)
    stop_words=LoadStopwords(dict_path)
    N_time=[]
    data=data[:15000]#减小数据量
    size=len(data)
    for n in N:
        p_list=[]
        m=Manager()
        result=m.list([])
        for i in range(n):#创建CPU内核数个进程
            p=Process(target=Map,args=(data[int(size/n*i):int(size/n*(i+1))],stop_words,result))
            p_list.append(p)
        start_time=time.time()
        for p in p_list:
            p.start()#启动进程
        for p in p_list:
            p.join()#阻滞主进程
        t=time.time()-start_time
        print('共用时: {}s'.format(t))#测试总用时
        N_time.append(t)
        print('{}进程处理完成'.format(n))
    Reduce(result,save_path)
    plt.plot(N,N_time)
    plt.show()