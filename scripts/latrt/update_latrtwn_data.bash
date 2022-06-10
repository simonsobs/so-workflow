LOCKFILE=/home/remyg/wn_calculation/latrt/latrtwhite_noises.lock                                               
LOGFILE=/home/remyg/wn_calculation/latrt/latrtwn_data.log                                                           
(                                                                                                   
    if [ -f $LOCKFILE ]; then                                                                       
        pid=`cat $LOCKFILE`                                                                         
        echo "- Process is already running with PID $pid."                                          
        exit                                                                                        
    else                                                                                            
        echo $BASHPID > $LOCKFILE                                                                   
    fi                                                                                              
                                                                                                    
    echo ============================================                                               
    echo SAT1 White noise: `date`                                                                       
    echo --------------------------------------------
    source /usr/share/modules/init/sh
    module use --append /mnt/so1/shared/modules/
    module load tod_stack
    cd /home/remyg/sotodlib
    module unload sotodlib
    python setup.py install --user > /dev/null
#     $(which python3) /home/remyg/wn_calculation/wn_monitor.py
    /mnt/so1/shared/software/anaconda3/200627/bin/python3 /home/remyg/wn_calculation/latrt/latrt_wn_monitor.py
    rm $LOCKFILE                                                                                    
    echo "Releasing lockfile"                                                                       
    echo ============================================                                               
) &>> $LOGFILE                                     