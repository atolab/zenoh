cd "$(dirname $0)"
basename=`basename $0`
filename="${basename%.*}"
outdir=${filename}_`date +"%y-%m-%d_%H-%M"`
mkdir $outdir
export ZENOD_VERBOSITY=debug

. proc_mgr.sh

echo "-------- START test $filename"

runproc zenohd $outdir zenohd.exe
zenohd=$?

sleep 1

runproc zenohc_sub $outdir zenohc.exe
sub=$?

echo "open" > ${proc_in[$sub]}
echo "dres 10 //test/res1" > ${proc_in[$sub]}
echo "dsub 10" > ${proc_in[$sub]}

sleep 1 

runproc zenohc_pub $outdir zenohc.exe
pub=$?

echo "open" > ${proc_in[$pub]}
echo "write //test/res1 MSG" > ${proc_in[$pub]}

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
