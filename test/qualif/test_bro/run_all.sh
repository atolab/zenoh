cd "$(dirname $0)"

echo "====== START bro tests"

./test_bro_2b_1/test_bro_2b_1.sh && \
./test_bro_2b_2/test_bro_2b_2.sh && \
./test_bro_2b_3/test_bro_2b_3.sh && \
./test_bro_2b_4/test_bro_2b_4.sh && \
./test_bro_14b_1/test_bro_14b_1.sh && \
./test_bro_14b_2/test_bro_14b_2.sh && \
./test_bro_14b_3/test_bro_14b_3.sh && \
./test_bro_14b_4/test_bro_14b_4.sh && \
./test_bro_50b_1/test_bro_50b_1.sh && \
./test_bro_50b_2/test_bro_50b_2.sh

if [ $? -eq 0 ]
then
    echo "[OK]"
    echo "====== END bro tests"
    echo ""
    exit 0
else
    echo "[ERROR]"
    echo "====== END bro tests"
    echo ""
    exit -1
fi
