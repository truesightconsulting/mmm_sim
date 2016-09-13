import sys
#client_path=sys.argv[1]
#main_path=sys.argv[2]
#mmm_id=int(sys.argv[3])
#client_id=int(sys.argv[4])
#db_server=sys.argv[5]
#db_name=sys.argv[6]
#port=int(sys.argv[7])
#username=sys.argv[8]
#password=sys.argv[9]

client_path='c:/Users/XinZhou/Documents/GitHub/demo/'
main_path='c:/Users/XinZhou/Documents/GitHub/mmm_sim/'
mmm_id=1
client_id=31
# DB server info
is_staging=True
db_server="bitnami.cluster-chdidqfrg8na.us-east-1.rds.amazonaws.com"
db_server="127.0.0.1"
db_name="nviz"
port=3306
if is_staging:
    username="root"
    password="bitnami"
else:
    username="Zkdz408R6hll"
    password="XH3RoKdopf12L4BJbqXTtD2yESgwL$fGd(juW)ed"
  
sys.path.append(main_path)
from mmm_main import mmm_main
mmm_main(client_path,main_path,mmm_id,client_id,is_staging,db_server,db_name,port,username,password)