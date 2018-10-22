cd "$(dirname $0)"
export ZENOD_VERBOSITY=debug

basename=`basename $0`
filename="${basename%.*}"
outdir=${filename}_`date +"%y-%m-%d_%H-%M"`
mkdir $outdir

. proc_mgr.sh
. graph_tools.sh

echo "-------- START test $filename"

run_brokers ../../../common/graph3 $outdir

sleep 1

runproc zenohc_sub1 $outdir zenohc.exe -p tcp/127.0.0.1:8022
sub1=$?

echo "open" > ${proc_in[$sub1]}
echo "dres 10 //test/res1" > ${proc_in[$sub1]}
echo "dsub 10" > ${proc_in[$sub1]}

sleep 1 

runproc zenohc_sub2 $outdir zenohc.exe -p tcp/127.0.0.1:8049
sub2=$?

echo "open" > ${proc_in[$sub2]}
echo "dres 10 //test/res1" > ${proc_in[$sub2]}
echo "dsub 10" > ${proc_in[$sub2]}

sleep 1 

runproc zenohc_sub3 $outdir zenohc.exe -p tcp/127.0.0.1:8026
sub3=$?

echo "open" > ${proc_in[$sub3]}
echo "dres 10 //test/res1" > ${proc_in[$sub3]}
echo "dsub 10" > ${proc_in[$sub3]}

sleep 1 

runproc zenohc_sub4 $outdir zenohc.exe -p tcp/127.0.0.1:8020
sub4=$?

echo "open" > ${proc_in[$sub4]}
echo "dres 10 //test/res1" > ${proc_in[$sub4]}
echo "dsub 10" > ${proc_in[$sub4]}

sleep 1 

runproc zenohc_pub1 $outdir zenohc.exe -p tcp/127.0.0.1:8050
pub1=$?

echo "open" > ${proc_in[$pub1]}
echo "dres 5 //test/res1" > ${proc_in[$pub1]}
echo "dpub 5" > ${proc_in[$pub1]}

sleep 1

runproc zenohc_pub2 $outdir zenohc.exe -p tcp/127.0.0.1:8004
pub2=$?

echo "open" > ${proc_in[$pub2]}
echo "dres 5 //test/res1" > ${proc_in[$pub2]}
echo "dpub 5" > ${proc_in[$pub2]}

sleep 1

runproc zenohc_pub3 $outdir zenohc.exe -p tcp/127.0.0.1:8044
pub3=$?

echo "open" > ${proc_in[$pub3]}
echo "dres 5 //test/res1" > ${proc_in[$pub3]}
echo "dpub 5" > ${proc_in[$pub3]}

sleep 1

echo "pub 5 MSG1" > ${proc_in[$pub1]}

echo "pub 5 MSG2" > ${proc_in[$pub2]}

echo "pub 5 MSG3" > ${proc_in[$pub3]}

sleep 1

cleanall

if [ `cat ${proc_log[$sub1]} | grep MSG1 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub1 didn't receive MSG1"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi
if [ `cat ${proc_log[$sub1]} | grep MSG2 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub1 didn't receive MSG2"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi
if [ `cat ${proc_log[$sub1]} | grep MSG3 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub1 didn't receive MSG3"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi

if [ `cat ${proc_log[$sub2]} | grep MSG1 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub2 didn't receive MSG1"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi
if [ `cat ${proc_log[$sub2]} | grep MSG2 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub2 didn't receive MSG2"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi
if [ `cat ${proc_log[$sub2]} | grep MSG3 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub2 didn't receive MSG3"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi

if [ `cat ${proc_log[$sub3]} | grep MSG1 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub3 didn't receive MSG1"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi
if [ `cat ${proc_log[$sub3]} | grep MSG2 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub3 didn't receive MSG2"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi
if [ `cat ${proc_log[$sub3]} | grep MSG3 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub3 didn't receive MSG3"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi

if [ `cat ${proc_log[$sub4]} | grep MSG1 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub4 didn't receive MSG1"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi
if [ `cat ${proc_log[$sub4]} | grep MSG2 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub4 didn't receive MSG2"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi
if [ `cat ${proc_log[$sub4]} | grep MSG3 | wc -l` -lt 1 ]
then 
  echo "[ERROR] sub4 didn't receive MSG3"
  echo "-------- END test $filename"
  echo ""
  exit -1
fi

echo "[OK]"
echo "-------- END test $filename"
echo ""
exit 0
