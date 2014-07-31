import numpy
import cPickle
import MySQLdb
from multiprocessing import Process
import monitor_conf as cnf
import ujson
import time
import mmap
import ctypes
import os
class current_spectra:
    
    def __init__(self, mode = 'c'):
        #print "initialising"
        self.which_db = 0
        self.last_timestamp = 0;
        self.db_connect = MySQLdb.connect(cnf.curr[0], cnf.curr[1], cnf.curr[2], cnf.curr[3])
        self.c_rfi = self.db_connect.cursor()
        self.c_rfi.execute("USE current_spectra")
        self.c_rfi.execute("SELECT * from which")
        res = self.c_rfi.fetchall()
        self.which_db=(res[0][0]+0)

        # self.fd = os.open('/tmp/mmaptest', os.O_CREAT | os.O_TRUNC | os.O_RDWR)
        # assert os.write(self.fd, '\x00' * mmap.PAGESIZE * 1024) == mmap.PAGESIZE * 1024
        # self.buf = mmap.mmap(self.fd, mmap.PAGESIZE * 1024, mmap.MAP_SHARED, mmap.PROT_WRITE)
        # self.current_spectrum = ctypes.c_char_p.from_buffer(self.buf)
        if mode == 'c':
            self.curr = numpy.memmap("/tmp/curr.dat", dtype="float32", mode='w+', shape = (cnf.modes[0]['n_chan'],))
            self.timestamp = numpy.memmap("/tmp/time.dat", dtype="int64", mode = 'w+', shape = (1,))
        else:
            self.curr = numpy.memmap("/tmp/curr.dat", dtype="float32", mode='r', shape = (cnf.modes[0]['n_chan'],))
            self.timestamp = numpy.memmap("/tmp/time.dat", dtype="int64", mode = 'r', shape = (1,))

    def insertSpectrum (self, spectrum, timestamp, mode):
        # print "pikckling"
        specS = cnf.Base64Encode(spectrum)
        # current_spectrum = ctypes.c_char_p.from_buffer(self.buf, 0)
        # print 'First 10 bytes of memory mapping: %r' % self.buf[:100]
        # current_spectrum = specS;
        # #assert current_spectrum.value == specS + '\0'
        # print "PAGESIXE =" + str(mmap.PAGESIZE)
        # print current_spectrum
        # # print current_spectrum.raw[:100]
        # print 'First 10 bytes of memory mapping: %r' % self.buf[:100]
        self.curr[:] = spectrum[:]
        self.timestamp[:] = numpy.array([timestamp,])[:]

        print "inserting"
        self.c_rfi.execute("INSERT IGNORE INTO spectra_%s (timestamp, spectrum, mode) values (%s,%s, %s)",(self.which_db, timestamp, specS, mode))
        # print "commiting"
        self.db_connect.commit()
        self.curr.flush()
        self.timestamp.flush()

    """Get the latest spectrum form the curren_spectra databse, the spectrum is returned in a base64 string format, to
    convert to a list use cnf.Base64Decode"""
    def getCurrentSpectrum(self):
        # self.c_rfi.execute("SELECT spectrum, timestamp from spectra_%i where timestamp = (SELECT max(timestamp) from spectra_%i)"%(self.which_db, self.which_db))
        # result = self.c_rfi.fetchone()
        # self.c_rfi.execute("FLUSH QUERY CACHE")

        result = [self.curr, self.timestamp[0]]

        print "current timestamp = %i"%self.timestamp[0]

        while self.last_timestamp == result[1]: #Current timestamp equals last timestamp
            result = [self.curr, self.timestamp[0]]
            time.sleep(0.1)
            # res = self.c_rfi.fetchall()
            # if res[0][0] != self.which_db: #Check if using the correct DB
            #     self.which_db = res[0][0]
            # while self.last_timestamp == result[1]:
            #     self.c_rfi.execute("SELECT spectrum, timestamp from spectra_%i where timestamp = (SELECT max(timestamp) from spectra_%i)"%(self.which_db, self.which_db))
            #     result = self.c_rfi.fetchone()
            #     self.c_rfi.execute("FLUSH QUERY CACHE")
            #     time.sleep(0.1)
        self.last_timestamp = result[1]
        return (result[0], result[1])

    def deleteOld(self, timestamp):
        p = Process(target=deleteProcess, args=(self.which_db,))
        self.which_db = (self.which_db + 1) % 2
        self.c_rfi.execute("UPDATE which SET table_no=%s",(self.which_db,))
        p.start()
        self.db_connect.commit()

    #Add channel number if you just want one channel
    def getRange (self,secs, endTime, mode, channel = -1):
        self.c_rfi.execute("SELECT spectrum, timestamp, mode from spectra_%s where timestamp <= %s ORDER BY timestamp ASC LIMIT %s",(self.which_db, endTime, secs))
        res = numpy.array(self.c_rfi.fetchall())
        times = res[:,1].astype(numpy.int)
        modes = res[:,2].astype(numpy.int)
        t_ind = numpy.where(modes == mode)
        indices = (times - res[:,1][0].astype(numpy.int))[t_ind]
        temp = numpy.empty([secs+1, cnf.modes[0]['n_chan']], dtype = numpy.float)
        timestamps = numpy.zeros([secs+1, cnf.modes[0]['n_chan']], dtype = numpy.int)
        print indices.shape
        # print indices.min()
        # print indices.max()
        numpy.put(timestamps, indices, times)
        for i in range(indices.shape[0]):
            temp[indices[i]] = cnf.Base64Decode(res[:,0][i])
        if channel != -1:
            return temp[:,channel], timestamps[:,channel]
        return temp, timestamps

    def close(self):
        self.c_rfi.close()

    def check_equal (self, arr1, arr2, dim1, dim2):
        for i in range(dim1):
            for j in range(dim2):
                if (arr1[i][j] != arr2[i][j]):
                    print "not equal at %i,%i"%(i,j)

def deleteProcess(which_db):
            db_connect = MySQLdb.connect(cnf.curr[0], cnf.curr[1], cnf.curr[2], cnf.curr[3])
            c_rfi = db_connect.cursor()
            c_rfi.execute("DELETE FROM spectra_%s", (which_db,))
            print "commiting"
            db_connect.commit()

if __name__ == "__main__":
    import time
    # import time
    # import matplotlib

    # import matplotlib.pyplot as plt

    # spec = current_spectra()
    # res = spec.getRange(100, 5);

    # print res[0].shape
    # print res[0]

    # matplotlib.use('TkAgg')

    spec = current_spectra()

    spectrum, times = spec.getRange(60, time.time(), 0)

    print "mode 0"
    print spectrum
    print times
    print times.shape

    spectrum, times = spec.getRange(60, time.time(), 1)

    print "mode 1"
    print spectrum
    print spectrum.shape
    print times
    print times.shape

    spectrum, times = spec.getRange(60, time.time(), 2)

    print "mode 2"
    print spectrum
    print times
    print times.shape

    spectrum, times = spec.getRange(60, time.time(), 3)

    print "mode 0"
    print spectrum
    print times
    print times.shape

    # res = spec.getCurrentSpectrum()

    # fig = plt.figure()
    # subplot1 = fig.add_subplot(1,1,1)

    # subplot1.cla()

    # subplot1.set_title('Spectrum at %s'%(time.ctime(res[1])))
    # subplot1.set_xlabel('Frequency (MHz)')
    # subplot1.set_ylabel('Level')
    

    # freqs = cnf.getFreqs(1) 
    # print res[0]
    # data = cnf.Base64Decode(res[0])



    # subplot1.plot(freqs/1.e6,data,'b')

    # fig.canvas.draw()
    # fig.show()

    # import time
    # while (True):
    #     time.sleep(2)
    #     res = spec.getCurrentSpectrum()

    #     fig = plt.figure()
    #     subplot1 = fig.add_subplot(1,1,1)

    #     subplot1.cla()

    #     subplot1.set_title('Spectrum at %s'%(time.ctime(res[1])))
    #     subplot1.set_xlabel('Frequency (MHz)')
    #     subplot1.set_ylabel('Level')


    #     freqs = cnf.getFreqs(1) 
    #     print res[0]
    #     data = cnf.Base64Decode(res[0])



    #     subplot1.plot(freqs/1.e6,data,'b')

    #     fig.canvas.draw()
        #fig.show()
    

    # test = numpy.arange (cnf.modes[0]['n_chan'], dtype="float64")
    # print "inserting"
    # spec.insertSpectrum(test, 15, 0)
    # print "inserted"

    # print spec.getCurrentSpectrum()
    

    # for i in range (15):
    #   test = [(600 + i) for j in range(14200)]
    #   spec.insertSpectrum(numpy.array(test), 5600 + i)
    #   if spec.getCurrentSpectrum()[0][0] != 600 + i:
    #       print "current spectra incorrect values = %i, should equal %i"%(spec.getCurrentSpectrum()[0],600 + i)

    # temp, timestamps = spec.getRange(1000)
    # print "len(timestamps) = %i"%len(timestamps)

    # print type(timestamps[0])
    # print timestamps[0]
    # print int (timestamps[0])

    # print temp[0].min()
    # print temp[:,0:100].min()
    # print temp[:,0:100].max()
    # print temp[0][0].shape

    spec.close()









