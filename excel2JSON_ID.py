# -*- coding: utf-8 -*-
"""
Created on Mon Jan  7 17:57:43 2019

@author: Administrator
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jan  3 11:15:33 2019

@author: Administrator
"""


## 每个sheet间，各个列的数据类型必须一致。
## (送检和报告)日期格式必须保证前十位为yyyy-mm-dd
## 病理号，住院号不能包含Nan
## 本脚本以base下的住院号合报告日期为合并依据。同一次有效病例的病理号和报告日期，以最后一次病理诊断（不含分子和淋巴结）为准

def todict(x):    
    tmp=x.groupby('指标名').apply(lambda x : ','.join(x.取值.unique())).reset_index()
    result=tmp[0]   
    result.index=tmp.指标名     
    return result.to_dict()

def pretreat(x):
    from datetime import datetime
    #x.住院号=x.住院号.apply(lambda i :i if type(i)==str else str(int(i)))
    #x.病理号=x.病理号.apply(lambda i :i if type(i)==str else str(int(i)))
#    x.报告日期=x.报告日期.apply(lambda i : datetime.strptime(str(datetime.strptime(i[:10],"%Y-%m-%d").date()),"%Y-%m-%d") \
#                        if type(i)==str else i)  
    x.报告日期=x.报告日期.apply(lambda i : datetime.strptime(str(i.date()),"%Y-%m-%d") \
                        if type(i)!= str \
                        else datetime.strptime(i[:10],"%Y-%m-%d"))
    return x
def excel2JSON_ID(path):
    print("loading pandas, json, tqdm...")   
     
    import pandas as pd
    import json
    from tqdm import tqdm   
    from math import isnan
    #date_format = "%Y-%M-%d"
    sheets=['base','emr','ihc','molecule','lymph','treatment','survival']    
    print('loading excel file (sheet names must contain all of base,emr,ihc,molecule and lymph)...')
    value_type={i:str for i in ['住院号','病理号','蜡块号','部位','指标名','取值']}
    dat_base,dat_emr,dat_ihc,dat_molecule,dat_lymph,dat_treat,dat_surv=[pretreat(pd.read_excel(path,sheet_name=i,dtype=value_type).dropna(subset=['住院号','病理号','取值','报告日期']).fillna('未知')) for i in sheets]

    print('unifying date format. ..')
    dat_base.loc[dat_base.指标名=='送检日期','取值']= \
    dat_base[dat_base.指标名=='送检日期'].取值.apply(lambda x:x[:10] if type(x)==str else str(x.date()))
    
    dat_base.loc[dat_base.住院号=='0','住院号']=dat_base.loc[dat_base.住院号=='0','病理号'].apply(lambda x : x+'-0')
    
    
    print('generating grouped info...')
    group_base=dat_base.groupby(['住院号','病理号','报告日期']).apply(todict).reset_index()
    group_emr=dat_emr.groupby(['住院号','病理号','部位']).apply(todict).reset_index()
    group_ihc=dat_ihc.groupby(['病理号','部位','蜡块号']).apply(todict).reset_index()
    group_molecule=dat_molecule.groupby(['住院号','报告日期','病理号']).apply(todict).reset_index()
    group_lymph=dat_lymph.groupby(['住院号','报告日期','病理号']).apply(todict).reset_index()
    group_treat=dat_treat.groupby(['住院号','报告日期','病理号']).apply(todict).reset_index()
    group_surv=dat_surv.groupby(['住院号','报告日期','病理号']).apply(todict).reset_index()
    
    emr_ihc=pd.merge(group_emr,group_ihc,on=['病理号','部位'],how='left',suffixes=['_emr','_ihc'])
    base_emr_ihc=pd.merge(group_base,emr_ihc,on=['住院号','病理号'],how='left',suffixes=['_base',''])
    #base_emr_ihc_molecule=pd.merge(base_emr_ihc,group_molecule,on='住院号',how='left',suffixes=['','_mol'])
    #base_emr_ihc_molecule_lymph=pd.merge(base_emr_ihc_molecule,group_lymph,on='住院号',how='left',suffixes=['_base','_lymph'])

    print('calculating valid cases...')    
    unique_id=group_base.住院号.unique()
    
    all=[]
    for i in tqdm(range(len(unique_id))):
        id=unique_id[i]        
        subs= base_emr_ihc[base_emr_ihc.住院号==id]        
        base={'住院号':id}
        const=subs[0].iloc[0]
        base.update({'性别':const.get('性别','未知'), '居住地':const.get('地区','未知'), '民族':const.get('民族','未知')})
        #pids=subs.病理号.unique()       
        one={'base_info':base}
        emr={}
        emr_one={}
        emr_pos={}
        for j in range(len(subs)):
            #pid=subs.loc[j,'病理号']
            #subs_pid=subs[subs.病理号==pid].dropna(subset=['0_emr'])
            subs_pid=pd.DataFrame(subs.iloc[j]).transpose()
            pid=subs_pid.病理号.iloc[0]
            if type(subs_pid['0_emr'].iloc[0])!=dict:
                pass
            else:
                emr={**{'报告日期':str(subs_pid['报告日期'].iloc[0].date())}, **subs_pid['0_emr'].iloc[0],\
                        **subs_pid[0].iloc[0],**{'患病部位':str(subs_pid['部位'].iloc[0])}}
                emr.pop('性别',None)
                emr.pop('地区',None)
                emr.pop('民族',None)
                ihc_Set=subs_pid.dropna(subset=['0_ihc'])
                if len(ihc_Set)==0:
                    emr=emr
                else:
                    posSet=ihc_Set['部位'].unique() 
                    ihc=[]
                    for k in range(len(posSet)):
                        tmp=subs_pid[subs_pid.部位==posSet[k]]
                        tmp_ihc=tmp['0_ihc'].iloc[0]
                        tmp_wax=tmp['蜡块号'].iloc[0]
                        tmp_ihc.update({'部位':posSet[k]})
                        tmp_ihc.update({'蜡块号':tmp_wax})
                        ihc.append(tmp_ihc)
                    emr.update({'免疫组化':ihc})
                emr.update({'病理号':pid})
                emr_one={'_'.join([subs_pid['部位'].iloc[0],str(j)]): emr}
                emr_pos.update(emr_one)
                
        one.update({'emr_info':emr_pos})
        
        
        if len(group_molecule)==0:
            one.update({'molecule_info':[]})
        else:  
            subs_molecule=group_molecule[group_molecule.住院号==id]
            if len(subs_molecule)==0:
                one.update({'molecule_info':[]})
            else:
                molecule=[]
                for l in range(len(subs_molecule)):
                    mole=subs_molecule.iloc[l,3]
                    mole.update({'报告日期':str(subs_molecule.iloc[l,1].date())})
                    mole.update({'编号':subs_molecule.iloc[l,2]})
                    molecule.append(mole)
                    one.update({'molecule_info':molecule})
                
        if len(group_lymph)==0:
            one.update({'lymph_info':[]})
        else:
            subs_lymph=group_lymph[group_lymph.住院号==id]
            if len(subs_lymph)==0:
                one.update({'lymph_info':[]})                
            else:
                lymph=[]
                for l in range(len(subs_lymph)):
                    lym=subs_lymph.iloc[l,3]
                    lym.update({'报告日期':str(subs_lymph.iloc[l,1].date())})
                    lym.update({'编号':subs_lymph.iloc[l,2]})
                    lymph.append(lym)
                    one.update({'lymph_info':lymph})
                    
        if len(group_treat)==0:
            one.update({'treat_info':[]})
        else:
            subs_treat=group_treat[group_treat.住院号==id]
            if len(subs_treat)==0:
                one.update({'treat_info':[]})                
            else:
                treat=[]
                for l in range(len(subs_treat)):
                    trt=subs_treat.iloc[l,3]
                    trt.update({'报告日期':str(subs_treat.iloc[l,1].date())})
                    trt.update({'编号':subs_treat.iloc[l,2]})
                    treat.append(trt)
                    one.update({'treat_info':treat})
                    
        if len(group_surv)==0:
            one.update({'surv_info':[]})
        else:
            subs_surv=group_surv[group_surv.住院号==id]
            if len(subs_surv)==0:
                one.update({'surv_info':[]})                
            else:
                surv=[]
                for l in range(len(subs_surv)):
                    srv=subs_surv.iloc[l,3]
                    srv.update({'报告日期':str(subs_surv.iloc[l,1].date())})
                    srv.update({'编号':subs_surv.iloc[l,2]})
                    surv.append(srv)
                    one.update({'surv_info':surv})
            
        all.append(one)    
             
    file=json.dumps(all,indent=True,ensure_ascii=False).encode('UTF-8') 
    
    print('saving to local file...')
    f=open(path+'_ID.JSON','wb')
    f.write(file)
    f.close()

## begin to json
path='D:/肺癌多中心/吉大一院/JiDaYiYuan20190319.xlsx'
excel2JSON_ID(path)
   
def dictMerge(arr):
    import pandas as pd    
    tmp=pd.concat([pd.DataFrame(list(i.items())) for i in arr])
    result=tmp.groupby([0]).apply(lambda x : '@'.join(x[1].unique()))    
    return dict(result)

def gap(x,d=14):
    import pandas as pd 
    x=sorted(x,reverse=True) 
    strides=[(i-j).days for i,j in zip(x[:(len(x)-1)], x[1:])]
    idx=[x//d for x in strides]
    idx.insert(0,0)# 计算日期间隔被时限整除的整数
    date_table=list(pd.DataFrame({'date':x,'idx':idx}).groupby('idx').apply(lambda x:x.date.max()))
    return date_table

def wax2dict(x):
    tmp=x.取值
    tmp.index=x.指标名
    result=tmp.to_dict()
    result.update({'蜡块号':x.蜡块号.iloc[0]})    
    return result

def validCase(path,days=30):    
    import json
    from tqdm import tqdm
    import re
    from datetime import datetime    
    js=json.load(open(path,'rb'))
    result=[]
    for i in tqdm(range(len(js))):
        case=js[i]
        date=[datetime.strptime(x,"%Y-%m-%d") for x in re.findall('[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}', str(case))]
        if date==[]:
            result.append(case)
        else:
            date_valid=gap(list(set(date)),days)
            emr=case['emr_info']
            molecule=case['molecule_info']
            lymph=case['lymph_info']
            single={'base_info': case['base_info']}
            for j in date_valid:
                index_emr={y:x for x,y in zip(emr.values(),emr.keys()) \
                           if abs((datetime.strptime(x['报告日期'],"%Y-%m-%d")-j).days)<=days}
                index_molecule=[x for x in molecule if abs((datetime.strptime(x['报告日期'],"%Y-%m-%d")-j).days)<=days]
                index_lymph=[x for x in lymph if abs((datetime.strptime(x['报告日期'],"%Y-%m-%d")-j).days)<=days]
                single.update({'emr_info':index_emr})
                single.update({'molecule_info':index_molecule})
                single.update({'lymph_info':index_lymph})
         
                result.append(single)
            
    file=json.dumps(result,indent=True,ensure_ascii=False).encode('UTF-8') 
    print('saving to local file...')
    f=open(path+'_validcase.JSON','wb')
    f.write(file)
    f.close()      
            
    
path='D:/肺癌多中心/山西肿瘤/山西肿瘤医院_20190312.xlsx_ID.JSON'
validCase(path)  
