cd "$(dirname $0)"
export ZENOD_VERBOSITY=debug

basename=`basename $0`
filename="${basename%.*}"
outdir=${filename}_`date +"%y-%m-%d_%H-%M"`
mkdir $outdir

. ./../../common/proc_mgr.sh

echo "-------- START test $filename"

while read line
do
  eval "$line"
  sleep 1
done <<< "$(./../../common/srvcmds.sh ../../common/graph2)"

sleep 1

runproc zenohc_sub zenohc.exe -p tcp/127.0.0.1:8005
sub=$?

echo "open" > ${proc_in[$sub]}
echo "dres 10 //test/res1" > ${proc_in[$sub]}
echo "dsub 10" > ${proc_in[$sub]}

sleep 1 

runproc zenohc_pub zenohc.exe -p tcp/127.0.0.1:8014
pub=$?

echo "open" > ${proc_in[$pub]}
echo "dres 5 //test/res1" > ${proc_in[$pub]}
echo "dpub 5" > ${proc_in[$pub]}

sleep 1

echo "pub 5 MSG" > ${proc_in[$pub]}

sleep 1

cleanall

if [ `cat ${proc_log[$sub]} | grep MSG | wc -l` -gt 0 ]
then 
  echo "[OK]"
  echo "-------- END test $filename"
  echo ""
  exit 0
else
  echo "[ERROR] sub didn't receive MSG"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi
