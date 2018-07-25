cd "$(dirname $0)"
sh init.sh
./test_c2b/test_c2b_1/test_c2b_1.sh \
&& ./test_c2b/test_c2b_2/test_c2b_2.sh \
&& ./test_c2b/test_c2b_3/test_c2b_3.sh \
&& ./test_c2b/test_c2b_4/test_c2b_4.sh \
&& ./test_c2b/test_c2b_5/test_c2b_5.sh 
retcode=$?

echo ""
if [ $? -eq 0 ]
then
    echo "[OK]"
    exit 0
else
    echo "[ERROR]"
    exit -1
fi