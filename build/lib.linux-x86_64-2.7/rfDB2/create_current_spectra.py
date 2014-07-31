import MySQLdb
import monitor_conf as cnf

print cnf.curr

cs = MySQLdb.connect(cnf.curr[0], cnf.curr[1], cnf.curr[2])

cs.autocommit(True)
csc = cs.cursor()

# create database to house processed RFI data
csc.execute('CREATE DATABASE IF NOT EXISTS current_spectra')
csc.execute('Use current_spectra')

#create table which
which_table = '''CREATE TABLE IF NOT EXISTS which (
table_no TINYINT DEFAULT 0 NOT NULL
)ENGINE=InnoDB;
'''

csc.execute(which_table)

csc.execute ("INSERT INTO which VALUES(0)")

cs.commit()

# create table SPECTRA0
spectra_table = '''CREATE TABLE IF NOT EXISTS spectra_0 (
timestamp INT UNSIGNED NOT NULL,
mode TINYINT NOT NULL,
spectrum MEDIUMTEXT NOT NULL,
PRIMARY KEY (mode, timestamp)
)ENGINE=InnoDB;
'''
csc.execute(spectra_table)
cs.commit()

# create table SPECTRA1
spectra_table = '''CREATE TABLE IF NOT EXISTS spectra_1 (
timestamp INT UNSIGNED NOT NULL,
mode TINYINT NOT NULL,
spectrum MEDIUMTEXT NOT NULL,
PRIMARY KEY (mode, timestamp)
)ENGINE=InnoDB;
'''

csc.execute(spectra_table)
cs.commit()

csc.close()
cs.close()
