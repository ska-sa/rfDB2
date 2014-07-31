import numpy
import cPickle
import MySQLdb
from multiprocessing import Process
import monitor_conf as cnf
import ujson
class current_spectra:
    
    def __init__(self):
        #print "initialising"
        self.which_db = 0
        self.last_timestamp = 0;
        self.db_connect = MySQLdb.connect(cnf.curr[0], cnf.curr[1], cnf.curr[2], cnf.curr[3])
        self.c_rfi = self.db_connect.cursor()
        self.c_rfi.execute("USE current_spectra")
        self.c_rfi.execute("SELECT * from which")
        res = self.c_rfi.fetchall()
        self.which_db=(res[0][0]+0)        

    def insertSpectrum (self, spectrum, timestamp, mode):
        # print "pikckling"
        specS = cnf.Base64Encode(spectrum)
        # print "inserting"
        self.c_rfi.execute("INSERT IGNORE INTO spectra_%s (timestamp, spectrum, mode) values (%s,%s, %s)",(self.which_db, timestamp, specS, mode))
        # print "commiting"
        self.db_connect.commit()

    """Get the latest spectrum form the curren_spectra databse, the spectrum is returned in a base64 string format, to
    convert to a list use cnf.Base64Decode"""
    def getCurrentSpectrum(self):
        self.c_rfi.execute("SELECT spectrum, timestamp from spectra_%i where timestamp = (SELECT max(timestamp) from spectra_%i)"%(self.which_db, self.which_db))
        result = self.c_rfi.fetchone()
        self.c_rfi.execute("FLUSH QUERY CACHE")

        if self.last_timestamp != result[1]: #Current timestamp equals last timestamp
            self.c_rfi.execute("SELECT * from which")
            res = self.c_rfi.fetchall()
            if res[0][0] != self.which_db: #Check if using the correct DB
                self.which_db = res[0][0]
                self.c_rfi.execute("SELECT spectrum, timestamp from spectra_%i where timestamp = (SELECT max(timestamp) from spectra_%i)"%(self.which_db, self.which_db))
                result = self.c_rfi.fetchone()
                self.c_rfi.execute("FLUSH QUERY CACHE")


        print "Got %i from db%i"%(result[1], self.which_db)
        self.last_timestamp = result[1]
        return (result[0], result[1])

    def deleteOld(self, timestamp):
        p = Process(target=deleteProcess, args=(self.which_db,))
        self.which_db = (self.which_db + 1) % 2
        self.c_rfi.execute("UPDATE which SET table_no=%s",(self.which_db,))
        p.start()
        self.db_connect.commit()

    #Add channel number if you just want one channel
    def getRange (self,secs, channel = -1):
        self.c_rfi.execute("SELECT spectrum, timestamp from spectra where timestamp <= (SELECT max(timestamp) from spectra) ORDER BY timestamp DESC LIMIT %s",(secs,))
        res = numpy.array(self.c_rfi.fetchall())
        temp = numpy.empty([secs+1, 14200], dtype = numpy.float)
        print res.shape
        for i in range(res[:,0].size):
            temp[i] = cnf.Base64Decode(res[:,0][i])
        return temp, res[:,1].astype(numpy.int)

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
    import matplotlib

    import matplotlib.pyplot as plt
    matplotlib.use('TkAgg')

    spec = current_spectra()
    res = spec.getCurrentSpectrum()

    fig = plt.figure()
    subplot1 = fig.add_subplot(1,1,1)

    subplot1.cla()

    subplot1.set_title('Spectrum at %s'%(time.ctime(res[1])))
    subplot1.set_xlabel('Frequency (MHz)')
    subplot1.set_ylabel('Level')
    

    freqs = cnf.getFreqs(1) 
    print res[0]
    data = cnf.Base64Decode(res[0])



    subplot1.plot(freqs/1.e6,data,'b')

    fig.canvas.draw()
    fig.show()

    import time
    while (True):
        time.sleep(2)
        res = spec.getCurrentSpectrum()

        fig = plt.figure()
        subplot1 = fig.add_subplot(1,1,1)

        subplot1.cla()

        subplot1.set_title('Spectrum at %s'%(time.ctime(res[1])))
        subplot1.set_xlabel('Frequency (MHz)')
        subplot1.set_ylabel('Level')


        freqs = cnf.getFreqs(1) 
        print res[0]
        data = cnf.Base64Decode(res[0])



        subplot1.plot(freqs/1.e6,data,'b')

        fig.canvas.draw()
        #fig.show()
    

    # test = [0 for j in range(14200)]
    # print "inserting"
    # spec.insertSpectrum(numpy.array(test), 1396858084 + 300)
    # print "inserted"
    

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









