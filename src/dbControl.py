"""dbself.control, self.contains methods to self.control the rfimonitor database
    Copyright (C) 2012  Christopher Schollar

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>."""

import MySQLdb as mdb
import sys
import h5py
import os
import time
import numpy
import math
import pickle
import datetime
import monitor_conf as conf

def get_hour(timestamp):
    new_hour = [0,0,0,0,0,0,0,0,0]
    new_hour[0:4] = datetime.datetime.fromtimestamp(timestamp).timetuple()[0:4]
    start_timestamp = time.mktime(new_hour)
    end_timestamp = time.mktime((datetime.datetime.fromtimestamp(start_timestamp) + datetime.timedelta(hours=1)).timetuple())
    end_timestamp = start_timestamp+3600 # 1 hour in the future
    return start_timestamp, end_timestamp

def array_to_csv_file (data,headings, file_name):
    import csv
    print headings
    print data[0:5]
    
    out = numpy.vstack ((headings, data))

    f = open(file_name, 'w+')

    writer = csv.writer(f, delimiter = ',')
    for row in out:
        writer.writerow(row)
        writer

    f.close()

class dbControl:

    def __init__(self, host, userName, password, database):
        self.con = mdb.connect(host, userName, password, database)
        self.cur = self.con.cursor()
        self.cur.execute ("SET bulk_insert_buffer_size= 1024 * 1024 * 256")

    # def __init__(self):
    #     print "THIS SHOULD ONLY BE USED FOR TESTING"

    def close (self):
        self.con.close()

    def getDumpAve (self, startTime = 0, endTime = time.time(), lowFrequency = 0, highFrequency = 16384):
        """Returns the average power for each 1 hour dump"""
        data = self.cur.execute("SELECT spectrum.id, AVG(ave) FROM spectrum_averages JOIN spectrum ON spectrum.id = dump_id WHERE startTime > %s AND endTime < %s AND frequency_bin > %s AND frequency_bin < %s GROUP BY dump_id ORDER BY dump_id",(startTime, endTime, lowFrequency, highFrequency))
        print self.cur.fetchall()

    def getFreqAve (self, startTime = 0, endTime = time.time(), lowFrequency = 0, highFrequency = 16384):
        """Returns the average power for each frequency over all dumps in the db"""
        data = self.cur.execute("SELECT frequency_bin, AVG(ave) FROM spectrum_averages JOIN spectrum ON spectrum.id = dump_id WHERE startTime > %s AND endTime < %s AND frequency_bin > %s AND frequency_bin < %s GROUP BY frequency_bin ORDER BY frequency_bin",(startTime, endTime, lowFrequency, highFrequency))
        print self.cur.fetchmany(20)

    def getFreqDumpAve (self, startTime = 0, endTime = time.time(), lowFrequency = 0, highFrequency = 16384):
        data = self.cur.execute("SELECT spectrum.id, frequency_bin, AVG(ave) FROM spectrum_averages JOIN spectrum ON spectrum.id = dump_id WHERE startTime > %s AND endTime < %s AND frequency_bin > %s AND frequency_bin < %s GROUP BY spectrum.id, frequency_bin ORDER BY spectrum.id, frequency_bin",(startTime, endTime, lowFrequency, highFrequency))
        print self.cur.fetchmany(20)

    def insertDump (self, data, mode, f):
        meta = self.calcMetaData(data['calibrated_spectrum'])
        #serialSpectrum = pickle.dumps(spectrum)

        localtime = time.localtime(data['timestamp'])
        fileName = "%02i.h5"%(localtime[3]) #Filename is the hour of the observation in the month
        path = os.path.join('/home/chris/rfi_data', str(localtime[0]), "%02i"%localtime[1], "%02i"%localtime[2],'') #Filepath is the year followed by the month year/month/
        location = "%s%s"%(path,fileName)
        # print "adc_overrange = %s for timestamp %s"%(str(data['adc_overrange']), str(timestamp))

        values = (data['timestamp'], mode, meta['ave'], meta['min'], meta['max'], \
            meta['stdDev'], data['adc_overrange'], data['fft_overrange'], \
            data['adc_level'], data['ambient_temp'], data['adc_temp'])
        try:

            self.cur.execute("INSERT INTO spectra (timestamp, mode, mean, min, max, stdDev \
                , adc_overrange, fft_overrange, adc_level, ambient_temp, adc_temp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", values)

            index = localtime[4] * 60 + localtime[5]
            #print 'index = %i'%index

            f['spectra'][index] = data['calibrated_spectrum']
            f['mode'][index] = mode

        except mdb.IntegrityError as e:
            print "--->SQL Error: %s" % e
            print "duplicate, continuing"

        f.flush()
        self.con.commit() 

    def calc_hourly_data(self, timestamp, hourFile):
        from scipy.stats import itemfreq
        func_list = ['min', 'max', 'mean', 'std']
        modes = itemfreq(hourFile['mode'])

        for m in modes:
            if m[0] != 0:
                #-----------_Calc Stats------------------------
                start = time.time()
                coverage = m[1]/float(3600)
                print "coverage = %f"%coverage
                indices = numpy.where(hourFile['mode'] == m[0])
                means = numpy.mean(hourFile['spectra'], axis=0)
                mins = numpy.min(hourFile['spectra'], axis=0)
                maxs = numpy.max(hourFile['spectra'], axis=0)
                stdDevs = numpy.std(hourFile['spectra'], axis=0)
                print "stats took %is"%(time.time() - start)
                start = time.time()
                sorts = numpy.sort(hourFile['spectra'], axis=0)
                print "sorts took %is"%(time.time() - start)
                f, indices= self.get_month_file(timestamp, m[0]) #OPEN FILE

                #INSERT STATS
                start = time.time()
                f['coverage'][indices[0],indices[1]] = coverage
                f['min'][indices[0],indices[1]] = mins
                f['max'][indices[0],indices[1]] = maxs
                f['mean'][indices[0],indices[1]] = means
                f['stddev'][indices[0],indices[1]] = stdDevs
                f['median'][indices[0],indices[1]] = sorts[int(m[1] * 0.5)]
                f['perc99'][indices[0],indices[1]] = sorts[int(m[1] * 0.99)]
                f['perc95'][indices[0],indices[1]] = sorts[int(m[1] * 0.95)]
                f['perc90'][indices[0],indices[1]] = sorts[int(m[1] * 0.9)]
                f['perc10'][indices[0],indices[1]] = sorts[int(m[1] * 0.1)]
                f['perc5'][indices[0],indices[1]] = sorts[int(m[1] * 0.05)]
                f['perc1'][indices[0],indices[1]] = sorts[int(m[1] * 0.01)]

                print "insert took %is"%(time.time() - start)

                #save file
                f.flush()
                f.close()

    def get_month_file(self, timestamp, mode):
        localtime = time.localtime(timestamp)
        fileName = "%04i_%02i.h5"%(localtime[0],localtime[1]) #Filename is the hour of the observation
        path = os.path.join('%s/rfi_data'%conf.root_dir, str(localtime[0]),"%02i"%localtime[1],"mode%02i"%mode,'') #Filepath year/month/day of the observation
        location = "%s%s"%(path,fileName)
        print location
        if not os.path.exists(path):
            os.makedirs(path)
        if os.path.isfile(location):
            f = h5py.File(location, 'r+')
        else:
            from calendar import monthrange
            n_days = monthrange(localtime[0],localtime[1])[1]
            f = h5py.File(location, 'w')
            f.create_dataset('coverage', (n_days, 24), dtype=numpy.float32)
            f.create_dataset('min', (n_days, 24, conf.modes[mode-1]['n_chan']), dtype=numpy.float32, compression='gzip', compression_opts=4)
            f.create_dataset('max', (n_days, 24, conf.modes[mode-1]['n_chan']), dtype=numpy.float32, compression='gzip', compression_opts=4)
            f.create_dataset('mean', (n_days, 24, conf.modes[mode-1]['n_chan']), dtype=numpy.float32, compression='gzip', compression_opts=4)
            f.create_dataset('stddev', (n_days, 24, conf.modes[mode-1]['n_chan']), dtype=numpy.float32, compression='gzip', compression_opts=4)
            f.create_dataset('median', (n_days, 24, conf.modes[mode-1]['n_chan']), dtype=numpy.float32, compression='gzip', compression_opts=4)
            f.create_dataset('perc99', (n_days, 24, conf.modes[mode-1]['n_chan']), dtype=numpy.float32, compression='gzip', compression_opts=4)
            f.create_dataset('perc95', (n_days, 24, conf.modes[mode-1]['n_chan']), dtype=numpy.float32, compression='gzip', compression_opts=4)
            f.create_dataset('perc90', (n_days, 24, conf.modes[mode-1]['n_chan']), dtype=numpy.float32, compression='gzip', compression_opts=4)
            f.create_dataset('perc1', (n_days, 24, conf.modes[mode-1]['n_chan']), dtype=numpy.float32, compression='gzip', compression_opts=4)
            f.create_dataset('perc5', (n_days, 24, conf.modes[mode-1]['n_chan']), dtype=numpy.float32, compression='gzip', compression_opts=4)
            f.create_dataset('perc10', (n_days, 24, conf.modes[mode-1]['n_chan']), dtype=numpy.float32, compression='gzip', compression_opts=4)
        
        indices = (localtime[2],localtime[3]) #day,hour
        print "indices = %s"%str(indices)

        return f, indices


    def calcMetaData(self, data):
        ret = {}

        ave = numpy.mean(data)
        mini = numpy.min(data)
        maxi = numpy.max(data)
        stdDev = numpy.std(data)

        ret['ave'] = ave
        ret['min'] = mini
        ret['max'] = maxi
        ret['stdDev'] = stdDev

        # import rfi_event
        # window = 5
        # data = numpy.concatenate( ( numpy.linspace( ave, ave, window ), data, numpy.linspace( ave, ave, window ) ) )

        # ret['RFI_mask'] = rfi_event.FD_median_filter( data, window = window )

        return ret

    #-------------------------------------------------------------------------------------GET----------------------------------------------------------------------------------

    def get_std_dev(self, startTime, channel):
        self.cur.execute("SELECT datum.value from datum inner join spectra on datum.spectra_id = spectra.id where timestamp = %s and type = %s and element_id = %s", (startTime, 'std', channel))
        result = self.cur.fetchone()[0]
        return result

    def get_archive (self, startTime, endTime, type):
        self.cur.execute("SELECT datum.value from datum inner join spectra on datum.spectra_id = spectra.id where timestamp >= %s and timestamp < %s and type = %s", (startTime, endTime, type))
        result = numpy.reshape(numpy.array(self.cur.fetchall()), (14200))
        return result

    def rfi_archive_get_period (self, startTime, endTime, type, frequency):
        import ratty1
        rat = ratty1.cam.spec()

        channel = rat.cal.freq_to_chan(frequency)

        self.cur.execute ("SELECT id FROM element WHERE channel = %s", (channel))
        elId = self.cur.fetchone()[0]
        print "elID"
        print elId
        self.cur.execute ("SELECT id FROM spectra WHERE timestamp >= %s and timestamp <= %s", (startTime, endTime))
        spectraIDs = numpy.array(self.cur.fetchall())[:,0]
        maxID = numpy.max(spectraIDs)
        minID = numpy.min(spectraIDs)
        print minID
        print maxID
        self.cur.execute ("SELECT datum.value FROM datum FORCE INDEX (spectra_id) WHERE datum.element_id = %s AND spectra_id >= %s AND spectra_id <= %s AND type = %s",
                            (elId, minID, maxID, type))
        result = numpy.array(self.cur.fetchall())
        return result

    def frequency_to_channel(self, frequency):
        import ratty1
        rat = ratty1.cam.spec()
        print "converting %fMHz and %f MHz to channel %i and %i"%(frequency * 1000000, rat.cal.config['ignore_low_freq'], rat.cal.freq_to_chan(frequency), rat.cal.freq_to_chan(rat.cal.config['ignore_low_freq']))
        return rat.cal.freq_to_chan(frequency * 1000000) - rat.cal.freq_to_chan(rat.cal.config['ignore_low_freq'])

    #enter unix timestamps for startTime and endTime, enter a particular channel number if you would like only 1 channel, enter a tuple channelRange = (lowchannel, highchannel)
    #if you would like a range of channels
    def rfi_monitor_get_range(self, startTime, endTime, mode_chan):
        
        # self.cur.execute ("SELECT DISTINCT fileLocation FROM spectra WHERE timestamp >= %s AND timestamp < %s", (startTime, endTime))
        # res = self.cur.fetchall()

        startTuple = datetime.datetime.fromtimestamp(startTime)
        endTuple = datetime.datetime.fromtimestamp(endTime)

        now = datetime.datetime.now()
        lastHour = datetime.datetime(now.year,now.month,now.day,now.hour)
        nHours = None

        timeDiff = endTuple - startTuple
        nSpectra = int(timeDiff.total_seconds())
        nHours = int(nSpectra/3600)
        if nSpectra % 3600 > 0:
            nHours += 1

        print "startTuple"
        print startTuple
        print "nHours = %i"%nHours

        fileName = "%02i.h5"%(startTuple.hour) #Filename is the hour of the observation
        path = os.path.join("%s/rfi_data"%conf.root_dir, str(startTuple.year), "%02i"%startTuple.month, "%02i"%startTuple.day,'') #Filepath year/month/day of the observation
        location = "%s%s"%(path,fileName)

        ret = None

        nSecs = startTuple.minute * 60 + startTuple.second

        print "nSpectra = %i"%nSpectra;

        ret = numpy.zeros((nSpectra,), dtype=numpy.float64)

        current = 0

        if endTuple > lastHour:
            current = 1

        mode = 0
        channel = -1
        indices = None

        try:
            f = h5py.File(location, 'r')
            print f['spectra'].shape
            for m_c in mode_chan:
                if m_c[0] in f['mode'][nSecs:3600]:
                    mode = m_c[0]
                    channel = m_c[1]
                    indices = numpy.where(f['mode'][nSecs -1:3600] == mode)
                elif indices == None:
                    indices = []

            ret[0:3600 - nSecs + 1][indices] = f['spectra'][nSecs - 1:3600,channel][indices]
            nSecs = 3600 - nSecs
            print "GOT %s"%location
            print "max = %f"%numpy.max(ret)
            print "min = %f"%numpy.min(ret)
        except IOError as e:
            nSecs = 3600 - nSecs
            print e
            print "NO FILE %s"%location

        currentTime = startTuple + datetime.timedelta(hours=1)

        print "endtuple = %s, lastHour = %s"%(str(endTuple),str(lastHour))

        for i in range(nHours - current):
            indices = None
            fileName = "%02i.h5"%(currentTime.hour) #Filename is the hour of the observation
            path = os.path.join("%s/rfi_data"%conf.root_dir, str(currentTime.year), "%02i"%currentTime.month, "%02i"%currentTime.day,'') #Filepath year/month/day of the observation
            location = "%s%s"%(path,fileName)
            print location
            print nSecs
            try:
                f = h5py.File(location, 'r')
                nS = 3600
                print "ret.shape[0]"
                print ret.shape[0]
                if (ret.shape[0] - nSecs < 3600):
                    nS = ret.shape[0] - nSecs
                for m_c in mode_chan:
                    print "m_c = %s"%str(m_c)
                    if m_c[0] in f['mode']:
                        mode = m_c[0]
                        channel = m_c[1]
                        indices = numpy.where(f['mode'][:nS] == mode)
                    elif indices == None:
                        indices = []

                print "indices"
                print indices
                if (ret.shape[0] != nSecs):
                    ret[nSecs:nSecs + nS][indices] = f['spectra'][:nS,channel][indices]
                nSecs += 3600
                print "max = %f"%numpy.max(ret)
                print "min = %f"%numpy.min(ret)
            except IOError as e:
                print e
                print location
                nSecs += 3600

            print "nSecs = %s"%nSecs

            currentTime = currentTime + datetime.timedelta(hours=1)

        print "nSPectra = %s"%nSpectra
        print "nSecs = %s"%nSecs

        if current == 1:
            print "IN current"
            import rfDB2.current_spectra as current_spectra
            curr = current_spectra.current_spectra(mode = 'r');
            print ("Getting %i mintues and %i seconds = %i seconds"%(endTuple.minute, endTuple.second, endTuple.minute * 60 + endTuple.second))
            spectra, times = curr.getRangeMem(endTuple.minute * 60 + endTuple.second, endTime, mode_chan)
            print spectra.shape
            print spectra.shape
            # print "ret shape = %s and spectra shape = %s and spectra[:,channel].shape = %s"%(str(ret[nSecs:].shape), str(spectra.shape), str(spectra[:,channel].shape))
            indices = times + int(endTuple.minute*60 + endTuple.second - endTime - 1)
            # print indices
            ret[nSecs:][indices] = spectra[:ret[nSecs:].shape[0]]

            # print "current min = %s and current max = %s"%(str(spectra[:,channel].min()), str(spectra[:,channel].max()))
            print ret[:5]
            print ret[-5:]
            print numpy.where(ret > 0.0)
            print (ret[numpy.where(ret > 0.0)])
            # ret[nSecs - 1:] = spectra[:,channel]

            # nSecs += spectra[:,channel].size
            curr.close()

        print "max = %f"%numpy.max(ret)
        print "min = %f"%numpy.min(ret)

        #Replace zeroes with mean
        # mean = numpy.mean(ret[ret != 0.0], axis = 0)
        # ret[ret==0.0] = mean

        #Interpolate zeroes
        if channel != -1:
            z = numpy.where(ret==0.0)[0]
            nz = numpy.where (ret!=0.0)[0]
            ret[ret==0.0]=numpy.interp(z,nz,ret[nz])

        z = numpy.where(ret > (100))[0]
        nz = numpy.where(ret < (100))[0]
        print z[0:5]
        print nz[0:5]
        ret[ret > 10 ** 2]=numpy.interp(z,nz,ret[nz])

        print "check"
        print "nSPectra = %s"%nSpectra
        print "nSecs = %s"%nSecs
        print ret.shape
        print "max = %f"%numpy.max(ret)
        print "min = %f"%numpy.min(ret)

        return ret


    def rfi_monitor_get_channel(self, channel, startTime, endTime):

        tup = datetime.datetime.now().timetuple[0:4]

        last_hour = datetime.datetime(tup[0],tup[1],tup[2],tup[3])

        if endTime > last_hour:
            #Need to use current_spectra
            if startTime > last_hour:
                #Only use current_spectra
                pass
            else:
                #Use current_spectra and rfimonitor
                pass
        else:
            #Use only rfimonitor
            pass

        start = time.localtime(startTime)
        end = time.localtime(endTime)



        #year, month, day, hour, 


        startPath = os.path.join(self.rootFilePath, str(start[0]), "%02i"%start[1], '')
        startFileName = "%02i_%02i_%02i.h5"%(localstart[2], localstart[3], localstart[4])
        startPath = os.path.join(self.rootFilePath, str(localstart[0]), "%02i"%localstart[1], '')
        startFileName = "%02i_%02i_%02i.h5"%(localstart[2], localstart[3], localstart[4])

    def get_last_ave_timestamp(self):
        self.cur.execute("SELECT MAX(timestamp) from spectra")
        result = self.cur.fetchone()
        return result[0]

    def get_ave_archive (self, type):
        self.cur.execute("SELECT element.frequency, AVG(datum.value) FROM datum  INNER JOIN element ON datum.element_id = element.id WHERE type = %s GROUP BY element.frequency ", (type))
        result = numpy.array(self.cur.fetchall()[:])[:,1]
        print result.shape
        return result

    def archive_get_val_at_time(self, time, typ):
        print "IN THE METHOD"
        starttime = self.get_last_ave_timestamp()
        print starttime
        self.cur.execute("SELECT id FROM spectra WHERE timestamp = %s",(starttime))
        result = self.cur.fetchone()
        self.cur.execute("SELECT datum.value, element.frequency FROM datum, element WHERE spectra_id = %s AND type = %s AND element.id = datum.element_id GROUP BY element.channel", (result[0], typ))
        result = numpy.array(self.cur.fetchall())
        result[:,1] = result[:,1]/1000000
        result[:,1] = numpy.around(result[:,1], decimals = 2)
        return result

    def archive_get_frequency_list(self):
        import ratty1
        rat = ratty1.cam.spec()
        low_frequency = rat.cal.config['ignore_low_freq']
        high_frequency = rat.cal.config['ignore_high_freq']
        print low_frequency
        print high_frequency
        self.cur.execute ("SELECT element.frequency FROM element WHERE element.frequency > %s AND element.frequency < %s",(low_frequency, high_frequency))
        result = numpy.array(self.cur.fetchall(), dtype=numpy.float32)
        result[:,0] = result[:,0]/1000000
        result[:,0] = numpy.around(result[:,0], decimals = 2)
        return result [:,0]

    def rfimonitor_get_adc_overrange(self, starttime, endtime):
        self.cur.execute("SELECT timestamp FROM spectra WHERE timestamp >= %s AND timestamp < %s AND adc_overrange = 1",(starttime, endtime))
        result = self.cur.fetchall()
        return result

    def rfimonitor_get_fft_overrange(self, starttime, endtime):
        self.cur.execute("SELECT timestamp FROM spectra WHERE timestamp >= %s AND timestamp < %s AND fft_overrange = 1",(starttime, endtime))
        result = self.cur.fetchall()
        print "FFT OVERRANGES"
        print len(result)
        return result

    def rfi_monitor_get_adc_overrange_pos (self, starttime, endtime):
        self.cur.execute("SELECT timestamp FROM spectra WHERE timestamp >= %s AND timestamp < %s AND adc_overrange = 1",(starttime, endtime))
        result = self.cur.fetchall()
        ret = [int(t[0] - starttime) for t in result]
        return ret

    def rfi_monitor_get_oldest_timestamp(self):
        self.cur.execute("SELECT min(timestamp) FROM spectra")
        result=self.cur.fetchone()
        return result[0]

    #-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    """Delete all spectra in rfimonitor db with timestamp <= timestamp"""
    def delete_spectra (self, timestamp):
        self.cur.execute("SELECT timestamp FROM rfimonitor.spectra WHERE timestamp <= %s ORDER BY timestamp",timestamp)
        res = self.cur.fetchall()
        location = "nothing"
        # print res[0:20]
        times = self.remove_duplicates(res)
        # print times
        for t in times:
            try:
                localtime = time.localtime(t)
                fileName = "%02i.h5"%(localtime[3]) #Filename is the hour of the observation in the month
                path = os.path.join('/home/chris/rfi_data', str(localtime[0]), "%02i"%localtime[1], "%02i"%localtime[2],'') #Filepath is the year followed by the month year/month/
                location = "%s%s"%(path,fileName)
                #d = datetime.datetime()
                # print "timestamp = %i"%t
                remove = time.mktime((localtime[0],localtime[1],localtime[2],localtime[3]+1,0,0,0,0,0))
                # print "remove = %i"%remove
                # print "diff = %i"%(remove - t)
                self.cur.execute("DELETE FROM rfimonitor.spectra where timestamp < %s", remove)
                os.remove(location)
                print "deleted %s"%location
                if localtime[3] == 0:
                    os.rmdir(path)
                if localtime[2] == 0:
                    os.rmdir(os.path.join('/home/chris/rfi_data', str(localtime[0]), "%02i"%localtime[1],""))

            except OSError:
                print "couldn't delete file %s"%location
                pass
        self.con.commit()

    def getSpectrum(self, timestamp):
        spectra = numpy.zeros(shape=(14200))
        try:
            localtime = time.localtime(timestamp)
            fileName = "%02i.h5"%(localtime[3]) #Filename is the hour of the observation in the month
            path = os.path.join('/home/chris/rfi_data', str(localtime[0]), "%02i"%localtime[1], "%02i"%localtime[2],'') #Filepath is the year followed by the month year/month/
            location = "%s%s"%(path,fileName)
            hour_start = time.mktime((localtime[0],localtime[1],localtime[2],localtime[3],0,0,0,0,0))
            f = h5py.File(location, mode='r')
            spectra = f['spectra'][timestamp - hour_start]
        except OSError:
                print "couldn't open file %s"%location
                pass
        return spectra

    def remove_duplicates(self, timestamps):
        ret = [timestamps[0][0]]
        for t in timestamps:
            if t[0] - ret[-1] >= 3600:
                ret.append(t[0])
        return ret

    def maintain_space(self):
        while self.check_space() < 0.05:
            print "Freeing space"
            self.cur.execute("select min(timestamp) from spectra")
            res = self.cur.fetchone()
            self.delete_spectra(res[0])

    def check_space (self):
        """Return percent of the harddrive is free"""
        s = os.statvfs("/")
        space = float(s.f_bavail)/s.f_blocks
        print "%f of harddrive free"
        return float(s.f_bavail)/s.f_blocks

if __name__ == '__main__':
    import ratty2
    import h5py
    import rfDB
    import json
    #import ratty1.cal as cal
    #import iniparse
    # rat = ratty1.cam.spec()
    # rat.connect()
    # rat.initialise()
    # monitor = dbControl("localhost", "root", "kat", "monitor")

    # mini = monitor.rfi_monitor_get_oldest_timestamp()
    # print "mini"
    # print mini
    # sTimestamp = int(time.time()) - 3600 * 2
    # eTimestamp = int(time.time())
    # sTime = time.localtime(sTimestamp)
    # eTime = time.localtime(eTimestamp)


    # res = monitor.rfi_monitor_get_range(sTimestamp, eTimestamp, 0, channel=250)

    # print "mode 0"
    # print res
    # print res.shape

    # res = monitor.rfi_monitor_get_range(sTimestamp, eTimestamp, 1, channel=250)
    # print "mode 1"
    # print res
    # print res.shape

    # res = monitor.rfi_monitor_get_range(sTimestamp, eTimestamp, 2, channel=250)
    # print "mode 2"
    # print res
    # print res.shape

    # res = monitor.rfi_monitor_get_range(sTimestamp, eTimestamp, 3, channel=250)
    # print "mode 3"
    # print res
    # print res.shape


    monitor = dbControl("localhost", "root", "kat", "monitor")
    f = h5py.File('/home/ratty2/monitor/rfi_data/2014/09/17/05.h5', mode='r+')

    monitor.calc_hourly_data(1410930000, f)
    # f.create_dataset('spectra', (3600, 14200))
    # spectrum, timestamp, last_cnt, stat = rat.getUnpackedData()
    # num_events = db.archive_get_val_at_time(int(time.time()) - 3600 - 3600, "num_events")
    # print"num_events"
    # print num_events[0:5]
    # check = [record for record in num_events]
    # print "check"
    # print check[0:5]
    # num_events_d = [dict(zip(('val','id'),record)) for record in num_events]
    # print num_events_d[0:5]
    # str_dict = json.dumps(num_events_d)
    # #print str_dict

    # print num_events.shape

    # monitor.closeDB()
