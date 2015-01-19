from smap import driver, util
import csv
import pyodbc
import time
from datetime import datetime
from datetime import timedelta

import time


class ReadODBC(driver.SmapDriver):


    def setup(self, opts):
        #Adding time series
        self.location = opts.get('Location', 'IIIT Delhi')

        self.add_timeseries('/Esito-GO','Units')
        self.add_timeseries('/Esito-No-GO','Units')
        self.add_timeseries('/Res_Esito-GO','Units')
        self.add_timeseries('/Res_Esito-No-GO','Units')
        self.add_timeseries('/Rot_Esito-GO','Units')
        self.add_timeseries('/Rot_Esito-No-GO','Units')
        self.add_timeseries('/Rig_Esito-GO','Units')
        self.add_timeseries('/Rig_Esito-No-GO','Units')
        self.add_timeseries('/Sur_Esito-GO','Units')
        self.add_timeseries('/Sur_Esito-No-GO','Units')

    def start(self):
        #Calling read_file method
        util.periodicSequentialCall(self.read_file).start(10)

    def read_file(self):
        ra = self.main_fn()

        for t in range(1, len(self.timed), 1):
            tim = self.timed[t]
            if(self.timed[t] > 1):
                #Adding read readings for each stream to be sent
                self.add('/Esito-GO',tim , self.esito_go[t])
                self.add('/Esito-No-GO',tim , self.esito_no_go[t])
                self.add('/Res_Esito-GO',tim , self.res_esito_go[t])
                self.add('/Res_Esito-No-GO',tim , self.res_esito_no_go[t])
                self.add('/Rot_Esito-GO',tim , self.rot_esito_go[t])
                self.add('/Rot_Esito-No-GO',tim , self.rot_esito_no_go[t])
                self.add('/Rig_Esito-GO',tim , self.rig_esito_go[t])
                self.add('/Rig_Esito-No-GO',tim , self.rig_esito_no_go[t])
                self.add('/Sur_Esito-GO',tim , self.sur_esito_go[t])
                self.add('/Sur_Esito-No-GO',tim , self.sur_esito_no_go[t])

                #Calling method to log previously sent readings
                self.write_prev_log( self.timed[t], self.esito_go[t], self.esito_no_go[t], self.res_esito_go[t], self.res_esito_no_go[t],
                    self.rot_esito_go[t], self.rot_esito_no_go[t], self.rig_esito_go[t], self.rig_esito_no_go[t], self.sur_esito_go[t], self.sur_esito_no_go[t], self.Ides[t])

    def main_fn(self):
        try:
            prevtime = 0000000001
            #Setting initial value for each stream
            self.init_ial()
            #Getting previously sent value for each stream
            ret =  self.set_prev_log()
            print 'ret', ret
            if (ret == 1):
                #If previous value found, Read access file for new readings
                rowes = self.read_access_file(self.timed[-1:][0], self.prev_id)
                for i in range(0, len(rowes)-1,  1):
                    tmp = datetime(rowes[i].Data.year, rowes[i].Data.month, rowes[i].Data.day, rowes[i].Ora.hour, rowes[i].Ora.minute, rowes[i].Ora.second)
                    timestamp = (tmp -  datetime(1970,1,1)).total_seconds()
                    if(prevtime == timestamp):
                        timestamp = timestamp + timectr
                        timectr = timectr + 1
                    else:
                        prevtime = timestamp
                        timectr = 1

                    timestamp = int(timestamp)
                    esito = rowes[i].Esito
                    res_esito = rowes[i].Res_Esito
                    rot_esito = rowes[i].Rot_Esito
                    rig_esito = rowes[i].Rig_Esito
                    sur_esito = rowes[i].Sur_Esito

                    self.timed.append(timestamp)
                    self.Ides.append(rowes[i].Id)
                    #Updating read value to arrays for each stream
                    if esito == 'GO':
                        self.esito_go.append(self.esito_go[-1:][0] + 1)
                        self.esito_no_go.append(self.esito_no_go[-1:][0])
                    elif esito == 'NOGO':
                        self.esito_go.append(self.esito_go[-1:][0])
                        self.esito_no_go.append(self.esito_no_go[-1:][0] + 1)
                    else:
                        self.esito_go.append(self.esito_go[-1:][0])
                        self.esito_no_go.append(self.esito_no_go[-1:][0])

                    if res_esito == 'GO':
                        self.res_esito_go.append(self.res_esito_go[-1:][0] + 1)
                        self.res_esito_no_go.append(self.res_esito_no_go[-1:][0])
                    elif res_esito == 'NOGO':
                        self.res_esito_go.append(self.res_esito_go[-1:][0])
                        self.res_esito_no_go.append(self.res_esito_no_go[-1:][0] + 1)
                    else:
                        self.res_esito_go.append(self.res_esito_go[-1:][0])
                        self.res_esito_no_go.append(self.res_esito_no_go[-1:][0])

                    if rot_esito == 'GO':
                        self.rot_esito_go.append(self.rot_esito_go[-1:][0] + 1)
                        self.rot_esito_no_go.append(self.rot_esito_no_go[-1:][0])
                    elif rot_esito == 'NOGO':
                        self.rot_esito_go.append(self.rot_esito_go[-1:][0])
                        self.rot_esito_no_go.append(self.rot_esito_no_go[-1:][0] + 1)
                    else:
                        self.rot_esito_go.append(self.rot_esito_go[-1:][0])
                        self.rot_esito_no_go.append(self.rot_esito_no_go[-1:][0])

                    if rig_esito == 'GO':
                        self.rig_esito_go.append(self.rig_esito_go[-1:][0] + 1)
                        self.rig_esito_no_go.append(self.rig_esito_no_go[-1:][0])
                    elif rig_esito == 'NOGO':
                        self.rig_esito_go.append(self.rig_esito_go[-1:][0])
                        self.rig_esito_no_go.append(self.rig_esito_no_go[-1:][0] + 1)
                    else:
                        self.rig_esito_go.append(self.rig_esito_go[-1:][0])
                        self.rig_esito_no_go.append(self.rig_esito_no_go[-1:][0])

                    if sur_esito == 'GO':
                        self.sur_esito_go.append(self.sur_esito_go[-1:][0] + 1)
                        self.sur_esito_no_go.append(self.sur_esito_no_go[-1:][0])
                    elif sur_esito == 'NOGO':
                        self.sur_esito_go.append(self.sur_esito_go[-1:][0])
                        self.sur_esito_no_go.append(self.sur_esito_no_go[-1:][0] + 1)
                    else:
                        self.sur_esito_go.append(self.sur_esito_go[-1:][0])
                        self.sur_esito_no_go.append(self.sur_esito_no_go[-1:][0])

        except Exception as exp:
            print 'Unknown Error: '+exp.__str__()

    def init_ial(self):
        #Initialize streams in for the first time
        self.timestamp = 0000000001
        self.timectr = 1
        self.timed=[0000000001]
        self.Ides=[0]
        self.esito_go = [0]
        self.esito_no_go = [0]
        self.res_esito_go = [0]
        self.res_esito_no_go = [0]
        self.rot_esito_go = [0]
        self.rot_esito_no_go = [0]
        self.rig_esito_go = [0]
        self.rig_esito_no_go = [0]
        self.sur_esito_go = [0]
        self.sur_esito_no_go = [0]

        self.prev_id=0
        self.prev_esito_go = 0
        self.prev_esito_no_go = 0
        self.prev_res_esito_go = 0
        self.prev_res_esito_no_go = 0
        self.prev_rot_esito_go = 0
        self.prev_rot_esito_no_go = 0
        self.prev_rig_esito_go = 0
        self.prev_rig_esito_no_go = 0
        self.prev_sur_esito_go = 0
        self.prev_sur_esito_no_go = 0



    def set_prev_log(self):
        try:
            #Reading previous values for each stream if present
            print 'Entered prev log'
            f = open('last_readings.csv','r')

            rowes = csv.reader(f)
            for row in rowes:
                print row
                self.prev_id=int(row[11])
                self.Ides[0]=int(row[11])
                self.timed[0] = int(row[0])
                self.esito_go[0]=int(row[1])
                self.esito_no_go[0]=int(row[2])
                self.res_esito_go[0] = int(row[3])
                self.res_esito_no_go[0]=int(row[4])
                self.rot_esito_go[0] = int(row[5])
                self.rot_esito_no_go[0]=int(row[6])
                self.rig_esito_go[0]=int(row[7])
                self.rig_esito_no_go[0]=int(row[8])
                self.sur_esito_go[0]=int(row[9])
                self.sur_esito_no_go[0]=int(row[10])
                return 1

        except Exception as e:
            print 'Error: Reading Latest Reading: '+e.__str__()
            return 0
            pass

    def read_access_file(self, tm, Id_s):
        try:
            #Function to read access file for reading and returns it as an access object
            tm = int(tm)
            dater = datetime.fromtimestamp(tm).strftime('%d/%m/%Y')
            timer = datetime.fromtimestamp(tm).strftime('%H:%M')
            print dater, timer
            cnxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb)};DBQ=\\\\OMG\NetworkSharedFolder\Misure.mdb;')
            cursor = cnxn.cursor()
            #query = 'SELECT TOP 100 Data, Ora, Esito, Res_Esito, Rot_Esito, Rig_Esito, Sur_Esito, Id from Misure WHERE Data > #'+dater+'# AND Ora > #'+timer+'# ORDER BY Data ASC, ORA ASC'
            query = 'SELECT TOP 100 Data, Ora, Esito, Res_Esito, Rot_Esito, Rig_Esito, Sur_Esito, Id from Misure WHERE Id > '+str(Id_s)+' ORDER BY Id ASC'
            print query
            cursor.execute(query)
            #cursor.execute('SELECT Id,Data, Ora, Esito, Res_Esito, Rot_Esito, Rig_Esito, Sur_Esito from Misure WHERE ID BETWEEN '+str(nope) + 'AND '+str(nope+100))
            rows = cursor.fetchall()
            return rows
        except Exception as ep:
            print 'Connection Error: '+ep.__str__()

    def write_prev_log(self, timers, esito_go, esito_no_go, res_esito_go, res_esito_no_go, rot_esito_go, rot_esito_no_go, rig_esito_go, rig_esito_no_go, sur_esito_go, sur_esito_no_go, id):
        try:
            #OverWriting previously sent value to csv log 
            f = open('last_readings.csv','w')

            row = str(timers)+','+str(esito_go)+','+str(esito_no_go)+','+str(res_esito_go)+','+str(res_esito_no_go)+','+str(rot_esito_go)+','+str(rot_esito_no_go)
            row = row +','+str(rig_esito_go)+','+str(rig_esito_no_go)+','+str(sur_esito_go)+','+str(sur_esito_no_go)+','+str(id)+'\n'

            print "Writing: "+row
            f.write(row)

        except Exception as e:
            print 'Error: Writing Latest Reading: '+e.__str__()
            pass

