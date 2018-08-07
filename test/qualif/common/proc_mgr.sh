proc_pid=""
proc_in=""
proc_log=""
lastproc=10

runproc()
{
  name=$1
  shift
  printf "run %-20s > %s\n" "$name" "$outdir/$name.log"
  lastproc=`expr $lastproc + 1`
  mkfifo $outdir/$name.in 
  eval "exec $lastproc<>$outdir/$name.in"
  $* < $outdir/$name.in > $outdir/$name.log 2>&1 &
  proc_pid[$lastproc]=$!
  proc_in[$lastproc]=$outdir/$name.in 
  proc_log[$lastproc]=$outdir/$name.log
  return $lastproc
}

cleanproc()
{
  eval "exec $1>&-"
  rm -f ${proc_in[$1]}
  kill -9 ${proc_pid[$1]}
}

cleanall()
{
  for i in $(seq 11 $lastproc)
  do
    cleanproc $i
  done
}
