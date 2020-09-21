import threading
import sys
import re
from contextlib import redirect_stdout
  
from kafka import KafkaConsumer

bootstrap_servers = ['localhost:9092'] 

def Gene_Scorecard(topic):
         matchID=topic 
         file_4=open("194161001-"+matchID+"-convertedcommentry.txt",'r')

         f = open("194161001-"+matchID+"-scorecard-computed.txt","w+")
         no_lines=0
         lines=file_4.readlines()
         for line in lines:
             prev=line
             no_lines=no_lines+1
         country_name=line.split(":")
         index_last=(lines.index(prev))-1
         opp_country_name=lines[index_last].split(":")
         player_List1=country_name[1].split(",")
         player_List2=opp_country_name[1].split(",")

         #============================================================================================================
         file_3=open("194161001-"+matchID+"-convertedcommentry.txt",'r')

         class Batsman:
            def __init__(self,name):
               self.Name=name
            R=0
            B=1
            M=0
            Fours=0
            Sixes=0
            SR=0
            c_wicket="not out                                 "
            def cause_wicket(self,c_w):
               c_w="{:<40}".format(c_w)
               self.c_wicket=self.c_wicket.replace(self.c_wicket,c_w)
            def runsscored(self,run_):
               self.R=int(self.R+run_)
            def ballsplayed(self,balls):
               self.B=self.B+balls
            def min_in_ground(self,minutes):
               self.M=self.M+int(minutes)
            def fours_hit(self,fours):
               self.Fours=self.Fours+fours
            def sixes_hit(self,sixes):
               self.Sixes=self.Sixes+sixes
            def strike_rate(self,ball,runs):
               self.SR=runs/ball
            def update_out_c(self,name):
               self.out_c=name
            def update_ball(self,bowl_name):
               self.out_bowl=bowl_name
            def display(self):
               self.SR=self.R/self.B
               self.SR=self.SR*100
               self.SR=round(self.SR,2)
               f.write("%-20s %-15s %-15s %-15s %-15s %-15s %-15s %-1s %s"%(str(self.Name),str(self.c_wicket),str(self.R),str(self.M),str(self.B),str(self.Fours),str(self.Sixes),str(self.SR),str("\n")))
         

         #================================================================TEAM============================================
         class Total:
            Total=0
            extra=0
            wicket=0
            wides=0
            legbyes=0
            freebyes=0
            noballs=0
            score=[]
            Name=[]
            wicket_over=[]
            score_1=[]
            Name_1=[]
            wicket_over_1=[]
            def fall_wicket_over(self,ov):
               self.wicket_over.append(ov)
            def wicket_fall_name(self,name):
               self.Name.append(name)
            def fall_wickets_score(self,sc):
               self.score.append(sc)
            def fall_wicket_over_1(self,ov_1):
               self.wicket_over_1.append(ov_1)
            def wicket_fall_name_1(self,name_1):
               self.Name_1.append(name_1)
            def fall_wickets_score_1(self,sc_1):
               self.score_1.append(sc_1)
            def byes_given(self,b):
               self.freebyes=self.freebyes+b
            def leg_byes(self,lb):
               self.legbyes=self.legbyes+lb
            def noball_deliveries(self,nb):
               self.noballs=self.noballs+nb
            def wides_deliveries(self,wd):
               self.wides=self.wides+wd
            def extra_given(self,ext):
               self.extra=self.extra+ext
            def total_score(self,run):
               self.Total=self.Total+run
            def total_wicket(self,wick):
               self.wicket=self.wicket+wick
            def display_total_score(self):
               f.write(self.Total)

         T1=Total()
         T2=Total()
         #===================================================================BOWLER============================================
         class Bowler:
                def __init__(self,name):
                  self.Name=name
                O=" "
                M=0
                B=0
                D=0
                R=0
                W=0
                f_s=0
                s_s=0
                ECON=0
                z_s=0
                s_s=0
                WD=0
                NB=0
                O_1=0
                O_2=" "
                ECON_1=0.0
                def dot_deliveries_mai(self,dots):
                    self.D=self.D+dots
                def deliveries_bowled(self,b):
                    self.B=self.B+b
                def over_bowled(self,bowl):
                    self.O=self.O+bowl
                def maidans_bowled(self,m_bowl):
                    self.M=self.M+m_bowl
                def runs_concided(self,c_runs):
                    self.R=self.R+c_runs
                def wickets_taken(self,wic):
                    self.W=self.W+wic
                def economy(self,ecnmy):
                    self.ECON=self.ECON+ecnmy
                def dot_balls(self,zo):
                    self.z_s=self.z_s+zo
                def fours_conceded(self,f):
                    self.f_s=self.f_s+f
                def sixes_conceded(self,s):
                    self.s_s=self.s_s+s
                def wides_given(self,w):
                    self.WD=self.WD+w
                def noball_given(self,n):
                    self.NB=self.NB+n
                def display(self):
                    self.O_1=int(self.B/6)       
                    self.O_2=round(self.B%6,1)
                    self.O=str(self.O_1)+"."+str(self.O_2)
                    self.ECON_1=self.R/float(self.O)
                    self.ECON=round(self.ECON_1,2)
                    f.write("%-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-1s %s"%(self.Name,str(self.O),str(self.M),str(self.R),str(self.W),str(self.ECON),str(self.z_s),str(self.f_s),str(self.s_s),str(self.WD),str(self.NB),str("\n"))) 

         Batsman_F=[]
         Bowler_F=[]
         Bowler_obj=[]
         batmen_obj=[]
         data = file_3.readlines()
         for ln,i in zip(data,range(0,len(data))):
               i=i+1
               if ln=='##########\n':
                  break
               statemnt=ln.split("&")
               ball_no=re.findall("^\d{1,2}\.\d$",statemnt[0])
               s=ball_no
               run=statemnt[1]
               y1=run 
               Bow_Bat_ln =statemnt[2].split(",")
               x = Bow_Bat_ln[0].split(" to ")      
               bow=x[0]
               y0=x[1] 
           
               if y0 not in Batsman_F:
                   Batsman_F.append(y0)
                   batmen_obj.append(Batsman(y0))
               if x[0] not in Bowler_F:
                    Bowler_F.append(x[0])
                    Bowler_obj.append(Bowler(x[0]))
               index_bow=Bowler_F.index(bow)
               index=Batsman_F.index(y0)       
               if y1=='0':
                    batmen_obj[index].runsscored(0)
                    batmen_obj[index].ballsplayed(1)
                    T1.total_score(0)
                    Bowler_obj[index_bow].runs_concided(0)
                    Bowler_obj[index_bow].deliveries_bowled(1)
                    Bowler_obj[index_bow].dot_balls(1)
               elif y1=='4':
                    batmen_obj[index].runsscored(4)
                    batmen_obj[index].ballsplayed(1)
                    batmen_obj[index].fours_hit(1)
                    T1.total_score(4)
                    Bowler_obj[index_bow].runs_concided(4)
                    Bowler_obj[index_bow].deliveries_bowled(1)
                    Bowler_obj[index_bow].fours_conceded(1)
               elif y1=='6':
                    Bowler_obj[index_bow].runs_concided(6)
                    Bowler_obj[index_bow].deliveries_bowled(1)
                    Bowler_obj[index_bow].sixes_conceded(1)
                    T1.total_score(6)
                    #print (T1.Total)
                    batmen_obj[index].runsscored(6)
                    batmen_obj[index].ballsplayed(1)
                    batmen_obj[index].sixes_hit(1)
               elif y1=='1':
                    batmen_obj[index].runsscored(1)
                    batmen_obj[index].ballsplayed(1)
                    T1.total_score(1)
                    #print (T1.Total)
                    Bowler_obj[index_bow].runs_concided(1)
                    Bowler_obj[index_bow].deliveries_bowled(1)
               elif y1=='2':
                    Bowler_obj[index_bow].runs_concided(2)
                    Bowler_obj[index_bow].deliveries_bowled(1)
                    T1.total_score(2)
                    #print (T1.Total)
                    batmen_obj[index].runsscored(2)
                    batmen_obj[index].ballsplayed(1)
               elif y1=='3':
                  Bowler_obj[index_bow].runs_concided(3)
                  Bowler_obj[index_bow].deliveries_bowled(1)
                  T1.total_score(3)
                  #print (T1.Total)
                  batmen_obj[index].runsscored(3)
                  batmen_obj[index].ballsplayed(1)
               elif y1=='1w':
                  T1.total_score(1)
                  #print (T1.Total)
                  T2.wides_deliveries(1)
                  T2.extra_given(1)
                  Bowler_obj[index_bow].runs_concided(1)
                  Bowler_obj[index_bow].wides_given(1)
                  # Bowler_obj[index_bow].extra_given(1)
               elif y1=='2w':
                  T1.total_score(2)
                  #print (T1.Total)
                  T2.wides_deliveries(2)
                  T2.extra_given(2)
                  Bowler_obj[index_bow].runs_concided(2)
                  Bowler_obj[index_bow].wides_given(1)
               elif y1=='3w':
                  T1.total_score(3)
                  #print (T1.Total)  
                  T2.extra_given(3)
                  T2.wides_deliveries(3)
                  Bowler_obj[index_bow].runs_concided(3)
                  Bowler_obj[index_bow].wides_given(1)
               elif y1=='4w':
                  T1.total_score(4)
                  #print (T1.Total)
                  T2.wides_deliveries(4)
                  T2.extra_given(4)
                  Bowler_obj[index_bow].runs_concided(4)
                  Bowler_obj[index_bow].wides_given(1) 
               elif y1=='1lb':
                  T1.total_score(1)
                  #print (T1.Total)
                  T2.leg_byes(1)
                  T2.extra_given(1)
                  batmen_obj[index].ballsplayed(1)
                  Bowler_obj[index_bow].deliveries_bowled(1)
                  Bowler_obj[index_bow].runs_concided(1)
               elif y1=='2lb':
                  T2.leg_byes(2)
                  T1.total_score(2)
                  #print (T1.Total)
                  T2.extra_given(2)
                  batmen_obj[index].ballsplayed(1)
                  Bowler_obj[index_bow].deliveries_bowled(1)
                  Bowler_obj[index_bow].runs_concided(2)
               elif y1=='3lb':
                  T2.leg_byes(3)
                  T1.total_score(3)
                  #print (T1.Total)
                  T2.extra_given(3)
                  batmen_obj[index].ballsplayed(1)
                  Bowler_obj[index_bow].deliveries_bowled(1)
                  Bowler_obj[index_bow].runs_concided(3)
               elif y1=='4lb':
                  T2.leg_byes(4)
                  T1.total_score(4)
                  #print (T1.Total)
                  T2.extra_given(4)
                  batmen_obj[index].ballsplayed(1)
                  Bowler_obj[index_bow].deliveries_bowled(1)
                  Bowler_obj[index_bow].runs_concided(4)
               elif y1=='1b':
                  T1.total_score(1)
                  #print (T1.Total)
                  T2.extra_given(1)
                  T2.byes_given(1)
                  batmen_obj[index].ballsplayed(1)
                  Bowler_obj[index_bow].deliveries_bowled(1)
                  Bowler_obj[index_bow].runs_concided(1)
               elif y1=='2b':
                  T1.total_score(2)
                  #print (T1.Total)
                  T2.extra_given(2)
                  T2.byes_given(2)
                  batmen_obj[index].ballsplayed(1)
                  Bowler_obj[index_bow].deliveries_bowled(1)
                  Bowler_obj[index_bow].runs_concided(2) 
               elif y1=='3b':
                  T1.total_score(3)
                  #print (T1.Total)
                  T2.extra_given(3)
                  T2.byes_given(3)
                  batmen_obj[index].ballsplayed(1)
                  Bowler_obj[index_bow].deliveries_bowled(1)
                  Bowler_obj[index_bow].runs_concided(3)
               elif y1=='4b':
                  T1.total_score(4)
                  #print (T1.Total)
                  T2.extra_given(4)
                  T2.byes_given(4)
                  batmen_obj[index].ballsplayed(1)
                  Bowler_obj[index_bow].deliveries_bowled(1)
                  Bowler_obj[index_bow].runs_concided(4)
               elif y1=='W':
                  T2.total_wicket(1)
                  Bowler_obj[index_bow].wickets_taken(1)
                  Bowler_obj[index_bow].deliveries_bowled(1)
                  l=next(file_3," ")
                  m=next(file_3," ")
                  # print (m)
                  if re.search(r'\sc\s[†]*[A-Z]*[a-z]*\s*[A-Z][a-z]* b [A-Z][a-z]* [A-Z]*[a-z]*',m):
                     n=re.search(r'\sc\s[†]*[A-Z]*[a-z]*\s*[A-Z][a-z]* b [A-Z][a-z]* [A-Z]*[a-z]*',m).group(0)
                     batmen_obj[index].cause_wicket(n)
                     #print (s)
                     #print (T1.Total)
                     T1.fall_wickets_score(T1.Total)
                     T1.wicket_fall_name(y0)
                     T1.fall_wicket_over(s)
                  elif re.search(r'c [A-Z]*[a-z]* b [A-Z]*[a-z]* [A-Z]*[a-z]*',m):
                     n6=re.search(r'c [A-Z]*[a-z]* b [A-Z][a-z]* [A-Z]*[a-z]*',m).group(0)
                     batmen_obj[index].cause_wicket(n6)
                     #print (s)
                     #print (T1.Total)
                     T1.fall_wickets_score(T1.Total)
                     T1.wicket_fall_name(y0)
                     T1.fall_wicket_over(s)   
                  elif re.search(r'st [†]*[A-Z][a-z]* [b] [A-Z][a-z]*',m):
                     n4=re.search(r'st [†]*[A-Z][a-z]* [b] [A-Z][a-z]*',m).group(0)
                     batmen_obj[index].cause_wicket(n4)
                     T1.fall_wickets_score(T1.Total)
                     #print (s)
                     #print (T1.Total)
                     T1.wicket_fall_name(y0)
                     T1.fall_wicket_over(s)
                  elif re.search(r'[c] & [b] [A-Z][a-z]* [A-Z]*[a-z]*',m):
                     n5=re.search(r'[c] & [b] [A-Z][a-z]* [A-Z]*[a-z]*',m).group(0)
                     batmen_obj[index].cause_wicket(n5)
                     T1.fall_wickets_score(T1.Total)
                     T1.wicket_fall_name(y0)
                     T1.fall_wicket_over(s)
                     #print (s)
                     #print (T1.Total)
                  elif re.search(r'run out *[A-Z][a-z]* [A-Z]*[a-z]* b [A-Z][a-z]* [A-Z]*[a-z]*',m):
                     n3=re.search(r'run out *[A-Z][a-z]* [A-Z]*[a-z]* b [A-Z][a-z]* [A-Z]*[a-z]*',m).group(0)
                     batmen_obj[index].cause_wicket(n)
                     T1.fall_wickets_score(T1.Total)
                     T1.wicket_fall_name(y0)
                     T1.fall_wicket_over(s)
                     #print (s)
                     #print (T1.Total)
                  elif re.search(r'b [A-Z]*[a-z]* [A-Z]*[a-z]*',m):
                     n1=re.search(r'b [A-Z]*[a-z]* [A-Z]*[a-z]*',m).group(0)
                     batmen_obj[index].cause_wicket(n1)
                     T1.fall_wickets_score(T1.Total)
                     T1.wicket_fall_name(y0)
                     T1.fall_wicket_over(s)
                     #print (s)
                     #print (T1.Total)
                  elif re.search(r'lbw b [A-Z][a-z]* [A-Z]*[a-z]*',m):
                     n2=re.search(r'lbw b [A-Z][a-z]* [A-Z]*[a-z]*',m).group(0)
                     batmen_obj[index].cause_wicket(n2)
                     T1.fall_wickets_score(T1.Total)
                     T1.wicket_fall_name(y0)
                     T1.fall_wicket_over(s)
                  if re.search(r'\d+m',m):
                     mi=re.search(r'\d+m',m).group(0)
                     mi=mi[:-1]
                     batmen_obj[index].min_in_ground(mi)
               elif y1=='1nb':
                  T2.noball_deliveries(1)
                  T1.total_score(1)
                  T2.extra_given(1)
                  Bowler_obj[index_bow].runs_concided(1)
                  Bowler_obj[index_bow].noball_given(1)
               elif y1=='2nb':
                  T2.noball_deliveries(2)
                  T1.total_score(2)
                  T2.extra_given(2)
                  Bowler_obj[index_bow].runs_concided(2)
                  Bowler_obj[index_bow].noball_given(1)
               elif y1=='3nb':
                  T2.noball_deliveries(3)
                  T1.total_score(3)
                  T2.extra_given(3)
                  Bowler_obj[index_bow].runs_concided(3)
                  Bowler_obj[index_bow].noball_given(1)
               elif y1=='4nb':
                  T2.noball_deliveries(4)
                  T1.total_score(4)
                  T2.extra_given(4)
                  Bowler_obj[index_bow].runs_concided(4)
                  Bowler_obj[index_bow].noball_given(1)
               elif y1=='5nb':
                  T2.noball_deliveries(5)
                  T1.total_score(5)
                  T2.extra_given(5)
                  Bowler_obj[index_bow].runs_concided(5)
                  Bowler_obj[index_bow].noball_given(1)
               elif y1=='6nb':
                  T2.noball_deliveries(6)
                  T1.total_score(6)
                  T2.extra_given(6)  
                  Bowler_obj[index_bow].runs_concided(6)
                  Bowler_obj[index_bow].noball_given(1)


         i=i+1
         Batsman_F_ins2=[]
         Bowler_F_ins2=[]
         Bowler_obj_ins2=[]
         batmen_obj_ins2=[]
         for j in range(i,len(data)):   
                  if data[j]=='##########\n':
                    break
                  statemnt_ins2=data[j].split("&")
                  #print (statemnt_ins2)
                  ball_no_ins2=re.findall("^\d{1,2}\.\d$",statemnt_ins2[0])
                  s_ins2=ball_no_ins2
                  run_ins2=statemnt_ins2[1]
                  y1_ins2=run_ins2 
                  Bow_Bat_ln_ins2 =statemnt_ins2[2].split(",")
                  x_ins2 = Bow_Bat_ln_ins2[0].split(" to ")
                  bow_ins2=x_ins2[0]
                  
                  y0_ins2=x_ins2[1] 
                  if y0_ins2 not in Batsman_F_ins2:
                        Batsman_F_ins2.append(y0_ins2)
                        batmen_obj_ins2.append(Batsman(y0_ins2))
                  if bow_ins2 not in Bowler_F_ins2:
                        Bowler_F_ins2.append(x_ins2[0])
                        Bowler_obj_ins2.append(Bowler(x_ins2[0]))
                  index_bow_ins2=Bowler_F_ins2.index(bow_ins2)
                  index_ins2=Batsman_F_ins2.index(y0_ins2)       
                  if y1_ins2=='0':
                        batmen_obj_ins2[index_ins2].runsscored(0)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                        T2.total_score(0)
                        #print  (T2.Total)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(0)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        Bowler_obj_ins2[index_bow_ins2].dot_balls(1)

                  elif y1_ins2=='4':
                        batmen_obj_ins2[index_ins2].runsscored(4)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                        batmen_obj_ins2[index_ins2].fours_hit(1)
                        T2.total_score(4)
                        #print  (T2.Total) 
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(4)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        Bowler_obj_ins2[index_bow_ins2].fours_conceded(1)
                  elif y1_ins2=='6':
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(6)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        Bowler_obj_ins2[index_bow_ins2].sixes_conceded(1)
                        T2.total_score(6)
                        #print  (T2.Total)     
                        batmen_obj_ins2[index_ins2].runsscored(6)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                        batmen_obj_ins2[index_ins2].sixes_hit(1)
                  elif y1_ins2=='1':
                        batmen_obj_ins2[index_ins2].runsscored(1)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                        T2.total_score(1)
                        #print  (T2.Total)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(1)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                  elif y1_ins2=='2':
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(2)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        T2.total_score(2)
                        #print  (T2.Total)
                        batmen_obj_ins2[index_ins2].runsscored(2)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                  elif y1_ins2=='3':
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(3)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        T2.total_score(3)
                        #print  (T2.Total)
                        batmen_obj_ins2[index_ins2].runsscored(3)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                  elif y1_ins2=='1b':
                        T2.total_score(1)
                        #print  (T2.Total)
                        T1.extra_given(1)
                        T1.byes_given(1)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(1)
                  elif y1_ins2=='2b':
                        T2.total_score(2)
                        #print  (T2.Total)
                        T1.extra_given(2)
                        T1.byes_given(2)
                        batmen_obj_ins2[index_ins2].ballsplayed(1) 
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(2)
                  elif y1_ins2=='3b':
                        T2.total_score(3)
                        #print  (T2.Total)
                        T1.extra_given(3)
                        T1.byes_given(3)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(3)
                  elif y1_ins2=='4b':
                        T2.total_score(4)
                        #print  (T2.Total)
                        T1.extra_given(4)
                        T1.byes_given(4)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(4)
                  elif y1_ins2=='1w':
                        T2.total_score(1)
                        #print  (T2.Total)
                        T1.wides_deliveries(1)
                        T1.extra_given(1) 
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(1)
                        Bowler_obj_ins2[index_bow_ins2].wides_given(1)
                        # Bowler_obj[index_bow].extra_given(1)
                  elif y1_ins2=='2w\n':
                        T2.total_score(2)
                        #print  (T2.Total)
                        T1.wides_deliveries(2)
                        T1.extra_given(2) 
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(2)
                        Bowler_obj_ins2[index_bow_ins2].wides_given(1)
                  elif y1_ins2=='3w':
                        T2.total_score(3)
                        #print  (T2.Total)
                        T1.wides_deliveries(3)
                        T1.extra_given(3) 
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(3)
                        Bowler_obj_ins2[index_bow_ins2].wides_given(1)
                  elif y1_ins2=='4w':
                        T2.total_score(4)
                        #print  (T2.Total)
                        T1.wides_deliveries(4)
                        T1.extra_given(4) 
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(4)
                        Bowler_obj_ins2[index_bow_ins2].wides_given(1)   
                  elif y1_ins2=='1lb':
                        T1.leg_byes(1)
                        T2.total_score(1)
                        #print  (T2.Total)
                        T1.extra_given(1)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(1)
                  elif y1_ins2=='2lb':
                        T1.leg_byes(2)
                        T2.total_score(2)
                        #print  (T2.Total)
                        T1.extra_given(2)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(2)
                  elif y1_ins2=='3lb':
                        T1.leg_byes(3)
                        T2.total_score(3)
                        #print  (T2.Total)
                        T1.extra_given(3)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(3)
                  elif y1_ins2=='4lb':
                        T1.leg_byes(4)
                        T2.total_score(4)
                        #print  (T2.Total)
                        T1.extra_given(4)
                        batmen_obj_ins2[index_ins2].ballsplayed(1)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(4)
                  elif y1_ins2=='W':
                        T1.total_wicket(1)
                        Bowler_obj_ins2[index_bow_ins2].wickets_taken(1)
                        Bowler_obj_ins2[index_bow_ins2].deliveries_bowled(1)
                        l_1=next(file_3," ")
                        m_1=next(file_3," ")
                        if re.search(r'st [†]*[A-Z][a-z]* [b] [A-Z][a-z]*',m_1):
                           n_4=re.search(r'st [†]*[A-Z][a-z]* [b] [A-Z][a-z]*',m_1).group(0)
                           batmen_obj_ins2[index_ins2].cause_wicket(n_4)
                           T2.fall_wickets_score_1(T2.Total)
                           T2.wicket_fall_name_1(y0_ins2)
                           T2.fall_wicket_over_1(s_ins2)
               
                        elif re.search(r'[c] & [b] [A-Z][a-z]* [A-Z]*[a-z]*',m_1):
                           n_3=re.search(r'[c] & [b] [A-Z][a-z]* [A-Z]*[a-z]*',m_1).group(0)
                           batmen_obj_ins2[index_ins2].cause_wicket(n_3)
                           T2.fall_wickets_score_1(T2.Total)
                           T2.wicket_fall_name_1(y0_ins2)
                           T2.fall_wicket_over_1(s_ins2)
                        elif re.search(r'run out *[A-Z][a-z]* [A-Z]*[a-z]* b [A-Z][a-z]* [A-Z]*[a-z]*',m_1):
                           n_6=re.search(r'run out *[A-Z][a-z]* [A-Z]*[a-z]* b [A-Z][a-z]* [A-Z]*[a-z]*',m_1).group(0)
                           batmen_obj[index].cause_wicket(n_6)
                           T2.fall_wickets_score(T2.Total)
                           T2.wicket_fall_name_1(y0_ins2)
                           T2.fall_wicket_over(s_ins2)
                        elif re.search(r'lbw b [A-Z][a-z]* [A-Z]*[a-z]*',m_1):
                           n_2=re.search(r'lbw b [A-Z][a-z]* [A-Z]*[a-z]*',m_1).group(0)
                           batmen_obj_ins2[index_ins2].cause_wicket(n_2)
                           T2.fall_wickets_score_1(T2.Total)
                           T2.wicket_fall_name_1(y0_ins2)
                           T2.fall_wicket_over_1(s_ins2)
                        elif re.search(r'\sc\s[†]*[A-Z]*[a-z]*\s*[A-Z][a-z]* b [A-Z][a-z]* [A-Z]*[a-z]*',m_1):
                           n_0=re.search(r'\sc\s[†]*[A-Z]*[a-z]*\s*[A-Z][a-z]* b [A-Z][a-z]* [A-Z]*[a-z]*',m_1).group(0)
                           batmen_obj_ins2[index_ins2].cause_wicket(n_0)
                           T2.fall_wickets_score_1(T2.Total)
                           T2.wicket_fall_name_1(y0_ins2)
                           T2.fall_wicket_over_1(s_ins2)
                        elif re.search(r'c [A-Z]*[a-z]* b [A-Z]*[a-z]* [A-Z]*[a-z]*',m_1):
                           n_5=re.search(r'c [A-Z]*[a-z]* b [A-Z]*[a-z]* [A-Z]*[a-z]*',m_1).group(0)
                           batmen_obj_ins2[index_ins2].cause_wicket(n_5)
                           T2.fall_wickets_score_1(T2.Total)
                           T2.wicket_fall_name_1(y0_ins2)
                           T2.fall_wicket_over_1(s_ins2)
                        elif re.search(r'b [A-Z]*[a-z]* [A-Z]*[a-z]*',m_1):
                           n_1=re.search(r'b [A-Z]*[a-z]* [A-Z]*[a-z]*',m_1).group(0)
                           batmen_obj_ins2[index_ins2].cause_wicket(n_1)
                           T2.fall_wickets_score_1(T2.Total)
                           T2.wicket_fall_name_1(y0_ins2)
                           T2.fall_wicket_over_1(s_ins2)
                        if re.search(r'\d+m',m_1):
                           mi_1=re.search(r'\d+m',m_1).group(0)
                           mi_1=mi_1[:-1]
                           batmen_obj_ins2[index_ins2].min_in_ground(mi_1)  
                  elif y1_ins2=='1nb':
                        T1.noball_deliveries(1)
                        T2.total_score(1)
                        #print  (T2.Total)
                        T1.extra_given(1)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(1)
                        Bowler_obj_ins2[index_bow_ins2].noball_given(1)
                  elif y1_ins2=='2nb':
                        T1.noball_deliveries(2) 
                        T2.total_score(2)
                        #print  (T2.Total)
                        T1.extra_given(2)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(2)
                        Bowler_obj_ins2[index_bow_ins2].noball_given(1)
                  elif y1_ins2=='3nb':
                        T1.noball_deliveries(3)
                        T2.total_score(3)
                        #print  (T2.Total)
                        T1.extra_given(3)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(3)
                        Bowler_obj_ins2[index_bow_ins2].noball_given(1)
                  elif y1_ins2=='4nb':
                        T1.noball_deliveries(4) 
                        T2.total_score(4)
                        #print  (T2.Total)
                        T1.extra_given(4)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(4)
                        Bowler_obj_ins2[index_bow_ins2].noball_given(1)
                  elif y1_ins2=='5nb':
                        T1.noball_deliveries(5)
                        T2.total_score(5)
                        #print  (T2.Total)
                        T1.extra_given(5)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(5)
                        Bowler_obj_ins2[index_bow_ins2].noball_given(1)
                  elif y1_ins2=='6nb':
                        T1.noball_deliveries(6)
                        T2.total_score(6)
                        #print  (T2.Total)
                        T1.extra_given(6)
                        Bowler_obj_ins2[index_bow_ins2].noball_given(1)
                        Bowler_obj_ins2[index_bow_ins2].runs_concided(6)




         n=len(Batsman_F_ins2)
         n2=len(Bowler_F_ins2)
         Did_not_bat_ins1=[]
         Did_bat=[]

         for i in Batsman_F_ins2:    
            for j in range(0,len(player_List2)):
               if(i in player_List2[j]):
                     Did_bat.append(player_List2[j])

         for j in range(0,len(player_List2)):
            if player_List2[j] not in Did_bat:
                        Did_not_bat_ins1.append(player_List2[j])

         Did_bat2=[]
         Did_not_bat_ins2=[]

         for i in Batsman_F:
            for j in range(0,len(player_List1)):
               if(i in player_List1[j]):
                     Did_bat2.append(player_List1[j])


         for j in range(0,len(player_List1)):
            if player_List1[j] not in Did_bat2:
               Did_not_bat_ins2.append(player_List1[j])
         '''



         '''
         f.write("%-61s %-15s %-15s %-15s %-15s %-15s %s"%("Name","R","M","B","4s","6s","SR\n" ))
         f.write("\n")
         for i in range(n-1,-1,-1):
             batmen_obj_ins2[i].display()

         f.write("\n")
         f.write("Total\t\t\t\t\t\t\t\t\t\t\t\t"+str(T2.Total)+"-"+str(T1.wicket) )
         f.write("\n")
         f.write("Extras\t\t\t\t\t\t\t\t\t\t\t\t"+str(T1.extra)+"("+str(T1.legbyes)+"lb"+" "+str(T1.wides)+"wd"+" "+str(T1.freebyes)+"b"+" "+str(T1.noballs)+"nb"+")")
         f.write("\n")
         len_score2=len(T2.wicket_over_1)
         #len_name2=len(T2.Name_1)
         f.write("\nFall of Wickets:"+" " .join([str(len_score2-j)+"-"+str(T2.Total-T2.score_1[j])+"("+str(T2.Name_1[j])+","+str(T2.wicket_over_1[j])+"ov"+")" for j in range(len_score2-1,-1,-1)])) 
         f.write("\n")
         if T1.wicket!=10:
           f.write("\nDid not bat :"+"".join([i for i in Did_not_bat_ins1]))
         f.write("   ")  
         f.write("\n")
         

         f.write("%-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %s"%("Name","O","M","Runs","W","ECON","0s","4s","6s","WD's","NB\n"))

         for i in range(n2-1,-1,-1):
            Bowler_obj_ins2[i].display()

         n1=len(Batsman_F)
         n3=len(Bowler_F)
         f.write("\n")
         f.write("%-61s %-15s %-15s %-15s %-15s %-15s %s"%("Name","R","M","B","4s","6s","SR\n" )) 
         for i in range(n1-1,-1,-1):
             batmen_obj[i].display()

         f.write("\n")
         f.write("Total\t\t\t\t\t\t\t\t\t\t\t\t"+str(T1.Total)+"-"+str(T2.wicket)+"\n" )
         f.write("Extras\t\t\t\t\t\t\t\t\t\t\t\t"+str(T2.extra)+"("+str(T2.legbyes)+"lb"+" "+str(T2.wides)+"wd"+" "+str(T2.freebyes)+"b"+" "+str(T2.noballs)+"nb"+")")

         len_score=len(T1.wicket_over)
         #len_name=len(T1.Name)

         f.write("\nFall of Wickets:"+" " .join([str(len_score-i)+"-"+str(T1.Total-T1.score[i])+"("+str(T1.Name[i])+","+str(T1.wicket_over[i])+"ov"+")" for i in range(len_score-1,-1,-1)])) 
         f.write("\n")
         if T2.wicket!=10:
             f.write("Did not bat :"+"".join([i for i in Did_not_bat_ins2])) 

         f.write("\n")
         f.write("\n")

         f.write("%-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %s"%("Name","O","M","Runs","W","ECON","0s","4s","6s","WD's","NB\n"))

         for i in range(n3-1,-1,-1):
            Bowler_obj[i].display()

         f.write("\n")
         f.write("\n")
         f.close()
         file_4.close()    



def consumer1():
    topic = '4143' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:        
        f1=open('4143.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f1.write (message)
        f1.close()
        Gene_Scorecard(topic)


def consumer2():
    topic = '4144' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    
    for line in consumer:
        f2 = open('4144.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID :  %-2s Commentry :-%s" %(line.topic,message))
        f2.write (message)
        f2.close()
        Gene_Scorecard(topic)    

def consumer3():
    topic = '4145' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f3 = open('4145.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f3.write (message)
        f3.close()
        Gene_Scorecard(topic)

def consumer4():
    topic = '4146' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f4 = open('4146.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f4.write (message)
        f4.close()
        Gene_Scorecard(topic)

def consumer5():
    topic = '4147' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f5 = open('4147.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f5.write (message)
        f5.close()
        Gene_Scorecard(topic)

def consumer6():
    topic = '4148' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f6 = open('4148.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f6.write (message)
        f6.close()
        Gene_Scorecard(topic)

def consumer7():
    topic = '4149' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f7 = open('4149.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f7.write (message)
        f7.close()
        Gene_Scorecard(topic)

def consumer8():
    topic = '4150' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f8 = open('4150.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f8.write (message)
        f8.close()
        Gene_Scorecard(topic)

def consumer9():
    topic = '4151' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f9 = open('4151.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f9.write (message)
        f9.close()
        Gene_Scorecard(topic)

def consumer10():
    topic = '4152' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f10 = open('4152.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f10.write (message)
        f10.close()
        Gene_Scorecard(topic)

def consumer11():
    topic = '4153' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f11 = open('4153.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f11.write (message)
        f11.close()
        Gene_Scorecard(topic)

def consumer12():
    topic = '4154' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f12 = open('4154.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f12.write (message)
        f12.close()
        Gene_Scorecard(topic)

def consumer13():
    topic = '4155' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f13 = open('4155.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f13.write (message)
        f13.close()
        Gene_Scorecard(topic)

def consumer15():
    topic = '4157' 
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')   
    for line in consumer: 
        f15 = open('4157.txt','a')
        message = line.value.decode('utf-8')
        print ("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f15.write (message)
        f15.close()
        Gene_Scorecard(topic)

def consumer16():
    topic= '4158'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f16 = open('4158.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f16.write (message)
        f16.close()
        Gene_Scorecard(topic)  

def consumer17():
    topic= '4159'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f17 = open('4159.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f17.write (message)
        f17.close()
        Gene_Scorecard(topic)

def consumer18():
    topic= '4160'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f18 = open('4160.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f18.write (message)
        f18.close()
        Gene_Scorecard(topic)

def consumer19():
    topic= '4161'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f19 = open('4161.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f19.write (message)
        f19.close()
        Gene_Scorecard(topic)

def consumer20():
    topic= '4162'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f20 = open('4162.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f20.write (message)
        f20.close()
        Gene_Scorecard(topic)

def consumer21():
    topic= '4163'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f21 = open('4163.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f21.write (message)
        f21.close()
        Gene_Scorecard(topic)

def consumer22():
    topic= '4165'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f22 = open('4165.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f22.write (message)
        f22.close()
        Gene_Scorecard(topic)

def consumer23():
    topic= '4166'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f23 = open('4166.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f23.write (message)
        f23.close()
        Gene_Scorecard(topic)


def consumer24():
    topic= '4168'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f24 = open('4168.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f24.write (message)
        f24.close()
        Gene_Scorecard(topic)


def consumer25():
    topic= '4169'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f25 = open('4169.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f25.write (message)
        f25.close()
        Gene_Scorecard(topic)

def consumer26():
    topic= '4171'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f26 = open('4171.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f26.write (message)
        f26.close()
        Gene_Scorecard(topic)

def consumer27():
    topic= '4173'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f27 = open('4173.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f27.write (message)
        f27.close()
        Gene_Scorecard(topic)

def consumer28():
    topic= '4174'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f28 = open('4174.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f28.write (message)
        f28.close()
        Gene_Scorecard(topic)

def consumer29():
    topic= '4175'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f29 = open('4175.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f29.write (message)
        f29.close()
        Gene_Scorecard(topic)

def consumer30():
    topic= '4176'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f30 = open('4176.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f30.write (message)
        f30.close()
        Gene_Scorecard(topic)

def consumer31():
    topic= '4177'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f31 = open('4177.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f31.write (message)
        f31.close()
        Gene_Scorecard(topic)

def consumer32():
    topic= '4178'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f32 = open('4178.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f32.write (message)
        f32.close()
        Gene_Scorecard(topic)

def consumer33():
    topic= '4179'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f33 = open('4179.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f33.write (message)
        f33.close()
        Gene_Scorecard(topic)  

def consumer34():
    topic= '4180'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f34 = open('4180.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f34.write (message)
        f34.close()
        Gene_Scorecard(topic) 

def consumer35():
    topic= '4183'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f35 = open('4183.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f35.write (message)
        f35.close()
        Gene_Scorecard(topic)

def consumer36():
    topic= '4184'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f36 = open('4184.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f36.write (message)
        f36.close()
        Gene_Scorecard(topic)

def consumer37():
    topic= '4186'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f37 = open('4186.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f37.write (message)
        f37.close()
        Gene_Scorecard(topic)

def consumer38():
    topic= '4187'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f38 = open('4187.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f38.write (message)
        f38.close()
        Gene_Scorecard(topic)

def consumer39():
    topic= '4190'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f39 = open('4190.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f39.write (message)
        f39.close()
        Gene_Scorecard(topic)

def consumer40():
    topic= '4191'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f40 = open('4191.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f40.write (message)
        f40.close()
        Gene_Scorecard(topic)

def consumer41():
    topic= '4192'
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    for line in consumer:
        f41 = open('4192.txt','a')
        message = line.value.decode('utf-8')
        print("Match ID : %-2s Commentry :-%s" %(line.topic,message))
        f41.write (message)
        f41.close()
        Gene_Scorecard(topic)


t1 = threading.Thread(target=consumer1)
t2 = threading.Thread(target=consumer2)
t3 = threading.Thread(target=consumer3)
t4 = threading.Thread(target=consumer4)
t5 = threading.Thread(target=consumer5)
t6 = threading.Thread(target=consumer6)
t7 = threading.Thread(target=consumer7)
t8 = threading.Thread(target=consumer8)
t9 = threading.Thread(target=consumer9)
t10 = threading.Thread(target=consumer10)
t11 = threading.Thread(target=consumer11)
t12 = threading.Thread(target=consumer12)
t13 = threading.Thread(target=consumer13)
#t14 = threading.Thread(target=consumer14)
t15 = threading.Thread(target=consumer15)
t16 = threading.Thread(target=consumer16)
t17 = threading.Thread(target=consumer17)
t18 = threading.Thread(target=consumer18)
t19 = threading.Thread(target=consumer19)
t20 = threading.Thread(target=consumer20)
t21 = threading.Thread(target=consumer21)
t22 = threading.Thread(target=consumer22)
t23 = threading.Thread(target=consumer23)
t24 = threading.Thread(target=consumer24)
t25 = threading.Thread(target=consumer25)
t26 = threading.Thread(target=consumer26)
t27 = threading.Thread(target=consumer27)
t28 = threading.Thread(target=consumer28)
t29 = threading.Thread(target=consumer29)
t30 = threading.Thread(target=consumer30)
t31 = threading.Thread(target=consumer31)
t32 = threading.Thread(target=consumer32)
t33 = threading.Thread(target=consumer33)
t34 = threading.Thread(target=consumer34)
t35 = threading.Thread(target=consumer35)
t36 = threading.Thread(target=consumer36)
t37 = threading.Thread(target=consumer37)
t38 = threading.Thread(target=consumer38)
t39 = threading.Thread(target=consumer39)
t40 = threading.Thread(target=consumer40)
t41 = threading.Thread(target=consumer41)

t1.start()
t2.start()
t3.start()
t4.start()
t5.start()
t6.start()
t7.start()
t8.start()
t9.start()
t10.start()
t11.start()
t12.start()
t13.start()
#t14.start()
t15.start()
t16.start()
t17.start()
t18.start()
t19.start()
t20.start()
t21.start()
t22.start()
t23.start()
t24.start()
t25.start()
t26.start()
t27.start()
t28.start()
t29.start()
t30.start()
t31.start()
t32.start()
t33.start()
t34.start()
t35.start()
t36.start()
t37.start()
t38.start()
t39.start()
t40.start()
t41.start()
