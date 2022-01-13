module load tod_stack

dataset=simset1

stream_id=crate1slot1
python fake_smurfdata_streamer.py -o out --dataset $dataset \
       --srate 0.005 --nchan 10 \
       --nsamps-per-frame 1000 \
       --nframes 120 \
       --stream-id $stream_id

python fake_hkdata_streamer.py -o out --dataset $dataset
