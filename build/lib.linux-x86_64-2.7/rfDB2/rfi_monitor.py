import ratty2
from multiprocessing import Process, Pipe, Queue, Lock, Event
from multiprocessing import Pool
import sys
# import archive_rfi_spectra as arch
import rfi_event as rfie
import time
import os
import h5py
import numpy as np
import current_spectra as cs
import monitor_conf as cnf
import dbControl

import signal
import sys

numLoops = 10
mode = 2

db = None
p1 = None
p2 = None
p3 = None
p4 = None

def signal_handler(signal, frame):
    print "Exiting Cleanly"
    if db != None:
        db.close()
    if p1 != None:
        p1.terminate()
    if p2 != None:
        p2.terminate()
    if p3 != None:
        p3.terminate()
    if p4 != None:
        p4.terminate()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

def threadInsert (queue):
    db = dbControl.dbControl(cnf.monitor_db[0], cnf.monitor_db[1], cnf.monitor_db[2], cnf.monitor_db[3])
    current = cs.current_spectra()
    count = numLoops
    last_cnt = 0
    last_day = -1;
    data = queue.get()
    localtime = time.localtime(data['timestamp'])
    fileName = "%02i.h5"%(localtime[3]) #Filename is the hour of the observation
    path = os.path.join('%s/rfi_data'%cnf.root_dir, str(localtime[0]), "%02i"%localtime[1], "%02i"%localtime[2],'') #Filepath year/month/day of the observation
    location = "%s%s"%(path,fileName)
    if not os.path.exists(path):
        os.makedirs(path)
    f = h5py.File(location, 'a')
    try:
        f.create_dataset('spectra', (3600, cnf.modes[mode-1]['n_chan']),chunks = (4, cnf.modes[mode-1]['n_chan']), dtype = np.float32, compression='gzip', compression_opts=4)
        f.create_dataset('mode', (3600,), dtype=np.int8)
    except:
        print "file exists"
    last_hour = localtime[3]
    last_timestamp = data['timestamp']
    hourstart, hourend = dbControl.get_hour(data['timestamp'])
    count = 0
    while True:
        if (last_hour != localtime[3]): #If new hour
            #Save file
            f.flush()

            #Archive last hour
            p3 = Process(target=threadArchive, args = (last_timestamp, f ,archive_added_event))
            p3.start()

            

            #Event to synchronise archive and rfi detection
            #archive_added_event = Event()

            

            #Detect RFI and archive
            # p4 = Process(target=threadRFIDetection, args = (last_timestamp,archive_added_event))
            # p4.start()

            #Make sure at least 25% of harddrive is free
            #db.maintain_space()

            #Create new file for next hour
            fileName = "%02i.h5"%(localtime[3]) #Filename is the hour of the observation
            path = os.path.join('%s/rfi_data'%cnf.root_dir, str(localtime[0]), "%02i"%localtime[1], "%02i"%localtime[2],'') #Filepath year/month/day of the observation
            location = "%s%s"%(path,fileName)
            if not os.path.exists(path):
                os.makedirs(path)
            f = h5py.File(location, mode='w')
            f.create_dataset('spectra', (3600, cnf.modes[mode -1]['n_chan']),chunks = (4, cnf.modes[mode -1]['n_chan']), dtype = np.float32, compression='gzip', compression_opts=4)
            f.create_dataset('mode', (3600,), dtype=np.int8)

            #reset time keeping variables to current hour
            last_hour = localtime[3]
            last_timestamp = data['timestamp']
            current.deleteOld(last_timestamp)
        # print "inserting to current" 
        current.insertSpectrum(data['calibrated_spectrum'],data['timestamp'], mode)
        # print "inserting to rfimonitor"
        db.insertDump (data, mode, f)
        # print "Done Inserting'"
        data = queue.get()
        #print "Timestamp = %i"%data[1]
        localtime = time.localtime(data['timestamp'])
    db.closeDB()
    current.close()
    #f.close()

def threadArchive (timestamp, hourFile, archive_added_event):
    print "In archive thread"
    timeT = time.time()
    rf_archive = dbControl.dbControl(cnf.monitor_db[0], cnf.monitor_db[1], cnf.monitor_db[2], cnf.monitor_db[3])
    #rf_archive.connect()
    print "Connected to DB, Start processing"
    rf_archive.calc_hourly_data(timestamp, hourFile)
    rf_archive.exit()
    f.close()
    archive_added_event.set()
    print "exiting rf archival, took %i seconds"%(time.time()-timeT)

def threadRFIDetection(timestamp, archive_added_event):
    print "In RFIDetection"
    timeT = time.time()
    num_events, len_events = rfie.rfi_detection(timestamp)
    print "Completed RFIDetection, took %i seconds"%(time.time() - timeT)
    archive_added_event.wait()
    time.sleep(6)
    print "entering insert to archive"
    rfie.insert_to_archive(timestamp, num_events, len_events)
    print "Added RFI data to archive"

    
def threadGetSpectrum (queue):
    rat = ratty2.cam.spec({'config_file':'%s/etc/ratty2/default_band%i'%(cnf.root_dir, mode)})

    rat.connect()
    rat.initialise(print_progress=True)
    count = 0
    start = 0
    while True:
        start = time.time()
        data = rat.get_spectrum()
        # print ("Time to get = %f"%(time.time() - start))
        # print "data collection took %i seconds"%(time.time() - start)
        #print "get adc_overrange = %s for timestamp %s"%(str(stat['adc_overrange']), str(timestamp))

        start = time.time()
        queue.put(data)

        # print ("Time to put = %f"%(time.time() - start))
        #print count
        # print data['calibrated_spectrum'].dtype

def run():

    print "start"
    queue = Queue()

    try:
        p = Process(target=threadGetSpectrum, args=(queue,))
        p2 = Process(target=threadInsert, args=(queue,))
        p.start()
        p2.start()
        print "p1 and p2 started"
        p.join()
        p2.join()
    except SystemExit, e:
        print "exiting"
        sys.exit(0)

if __name__ == "__main__":
    run()
    

    
