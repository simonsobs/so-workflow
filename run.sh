tag=simdata1

# simulating smurf data
for stream_id in crate{1..3}slot{1..3}; do
    python fake_smurfdata_streamer.py -o out --tag $tag \
           --srate 0.005 --nchan 10 \
           --nsamps-per-frame 1000 \
           --nframes 10 \
           --stream-id $stream_id
done

python fake_hkdata_streamer.py -o out --tag $tag \
       --srate 0.005 --nsamps-per-frame 10000
