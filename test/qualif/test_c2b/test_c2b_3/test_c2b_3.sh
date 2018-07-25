cd "$(dirname $0)"
basename=`basename $0`
filename="${basename%.*}"
outdir=${filename}_`date +"%y-%m-%d_%H-%M"`
mkdir $outdir

. ../../common/proc_mgr.sh

echo "==== Run test $filename ===="

runproc zenohd zenohd.exe
zenohd=$?

sleep 1

runproc zenohc_pub zenohc.exe
pub=$?

echo "open" > ${proc_in[$pub]}
echo "dres 5 //test/res1" > ${proc_in[$pub]}
echo "dpub 5" > ${proc_in[$pub]}

sleep 1

runproc zenohc_sub zenohc.exe
sub=$?

echo "open" > ${proc_in[$sub]}
echo "dres 10 //test/res1" > ${proc_in[$sub]}
echo "dsub 10" > ${proc_in[$sub]}

sleep 1

echo "pub 5 MSG" > ${proc_in[$pub]}
sleep 1

cleanall

if [ `cat ${proc_log[$sub]} | grep MSG | wc -l` -gt 0 ]
then 
  echo "[OK]"
  exit 0
else
  echo "[ERROR] zenohc_sub didn't receive MSG"
  exit -1
fi
