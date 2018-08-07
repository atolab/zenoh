cd "$(dirname $0)"

echo "====== START c2b tests"

./test_c2b_1/test_c2b_1.sh && \
./test_c2b_2/test_c2b_2.sh && \
./test_c2b_3/test_c2b_3.sh && \
./test_c2b_4/test_c2b_4.sh && \
./test_c2b_5/test_c2b_5.sh 
retcode=$?

if [ $? -eq 0 ]
then
    echo "[OK]"
    echo "====== END c2b tests"
    echo ""
    exit 0
else
    echo "[ERROR]"
    echo "====== END c2b tests"
    echo ""
    exit -1
fi
