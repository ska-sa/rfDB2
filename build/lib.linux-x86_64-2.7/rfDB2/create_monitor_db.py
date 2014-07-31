#! /usr/bin/env python

import MySQLdb
import monitor_conf

# db = MySQLdb.connect(host='192.168.2.2', port=3316, user='rfi')
arg = monitor_conf.monitor_db
db = MySQLdb.connect(arg[0],arg[1],arg[2])
db.autocommit(True)
dbc = db.cursor()

# create database to house processed RFI data
dbc.execute('CREATE DATABASE IF NOT EXISTS monitor')
dbc.execute('Use monitor')

# clone the SYSTEM table from the RFI monitor database NOT AGAIN, it was useless
# dbc.execute('CREATE TABLE IF NOT EXISTS monitor.system LIKE rfimonitor.system;')
# dbc.execute('SELECT * FROM rfimonitor.system;')
# dbc.execute('INSERT INTO current_spectra0.system SELECT * FROM rfimonitor.system;')
# dbc.execute('ALTER TABLE system ADD current bit(1);')
# dbc.execute('UPDATE system SET current=1;')
# db.commit()

# #create table mode
# mode_table = '''CREATE TABLE IF NOT EXISTS mode (
# id TINYINT NOT NULL,
# n_chan MEDIUMINT NOT NULL,
# bandwidth FLOAT NOT NULL,
# base_freq FLOAT NOT NULL,
# ignore_low_chan MEDIUMINT,
# ignore_high_chan MEDIUMINT,
# PRIMARY KEY (id)
# )
# '''
# dbc.execute(mode_table)
# db.commit()

# for mode in cnf.modes:
#     dbc.execute('''INSERT INTO mode (
#         id,
#         n_chan,
#         bandwidth,
#         base_freq)
#         VALUES (%s,%s,%s,%s,%s,%s)''',(
#         mode['id'],
#         mode['n_chan'],
#         mode['bandwidth'],
#         mode['base_freq']))

# #create mode_chan_freq table
# mode_chan_freq = '''CREATE TABLE IF NOT EXISTS mode_chan_freq (
# mode TINYINT NOT NULL,
# chan MEDIUMINT NOT NULL,
# frequency FLOAT NOT NULL)
# '''


# create table SPECTRA
spectra_table = '''CREATE TABLE IF NOT EXISTS spectra (
timestamp INT UNSIGNED NOT NULL,
mode TINYINT NOT NULL,
adc_overrange TINYINT NOT NULL,
fft_overrange TINYINT NOT NULL,
adc_level FLOAT NOT NULL,
ambient_temp FLOAT NOT NULL,
adc_temp FLOAT NOT NULL,
mean FLOAT,
min FLOAT,
max FLOAT,
stdDev FLOAT,
n_detected_RFI MEDIUMINT,
PRIMARY KEY (mode, timestamp)
)ENGINE=InnoDB;
'''
dbc.execute(spectra_table)
db.commit()

dbc.close()
db.close()

