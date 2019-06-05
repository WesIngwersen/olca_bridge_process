###Script to create a bridge processes in an openlca database. The contents of this file could be pasted into the Python interpreter in the openLCA database. The
#  processes and associated reference flows to which the bridge processes link should already be present in the openlca database before running the script. 

import csv
import java.util.UUID as UUID

#Path to csv file, user forward slash even for Windows
csvFile = 'C:/Users/Username/bridgeprocessdata.csv'

#Object to store the flows for each process
processFlows = {}

#Function to store the input flow found by the SQL query. If several matches are found, only the last one will be stored
def callbackInput(rs):
   processFlows['Input']=rs.getInt(1)

#Function to store the output flow found by the SQL query. If several matches are found, only the last one will be stored
def callbackOutput(rs):
   processFlows['Output']=rs.getInt(1)

#Function to get the input flow
#Modify it depending on the defined criteria (e.g. name, location, category, provider, etc.)
def find_flowInput(name):
    processFlows['Input'] = olca.querySql("SELECT tbl_flows.id FROM tbl_flows WHERE tbl_flows.name = '" + name + "'",callbackInput)

#Function to get the output flow. If several criteria apart from the name need to be fulfilled (e.g. location) a SQL query is run.
#Modify it depending on the defined criteria (e.g. name, location, category, provider, etc.)
def find_flowOutput(name):
    processFlows['Output'] = olca.querySql("SELECT tbl_flows.id FROM tbl_flows WHERE tbl_flows.name = '" + name + "'",callbackOutput)

#Function to create a new actor if any match for the name in the csv is found in the database
def create_actor(name):
    actor = Actor()
    actor.refId = UUID.randomUUID().toString()
    actor.name = name
    olca.insertActor(actor)

#Read the csv file and generate the processes
with open(csvFile,'rb') as f:
    data = csv.DictReader(f)
    for row in data:
        process = Process()
        process.documentation = ProcessDocumentation()
        process.refId = UUID.randomUUID().toString()
        process.processType = ProcessType.UNIT_PROCESS
        process.name = row['BridgeProcessName']
        process.description = row['BridgeProcessDescription']
        process.documentation.intendedApplication = row['BridgeProcessIntendedApplication']
        datasetOwner = row['BridgeProcessDatasetOwner']
        if datasetOwner != '':
            owner = olca.getActor(datasetOwner)
            if owner is None:
                create_actor(datasetOwner)
                owner = olca.getActor(datasetOwner)
            process.documentation.dataSetOwner = owner
        dataDocumentor = row['BridgeProcessDataDocumentor']
        if dataDocumentor != '':
            documentor = olca.getActor(dataDocumentor)
            if documentor is None:
                create_actor(dataDocumentor)
                documentor = olca.getActor(dataDocumentor)
            process.documentation.dataDocumentor = documentor
        #If more data were available in the csv file (e.g. category, time, geography, etc.), the correspondent process documentation fields should be completed
        olca.insertProcess(process)
        #If any flow is not found, an error is logged and its exchange will not be created
        find_flowInput(row['InputName'])
        if 'Input' not in processFlows:
          log.error('Flow: "' + row['InputName']  +' does not exist in database')
        find_flowOutput(row['OutputName'])
        if 'Output' not in processFlows:
          log.error('Flow: "' + row['OutputName'] +' does not exist in database')
        for key, value in processFlows.items():
            exchange = Exchange()
            exchange.flow = olca.getFlow(value)
            if key == 'Input':
                exchange.input = True
                exchange.amountValue = float(row['InputAmount'])
                unitCol = 'InputUnit'
            elif key == 'Output':
                exchange.input = False
                exchange.amountValue = float(row['OutputAmount'])
                unitCol = 'OutputUnit'
            for factor in exchange.flow.flowPropertyFactors:
                for unit in factor.flowProperty.unitGroup.getUnits():
                    if unit.name == row[unitCol]:
                        exchange.unit = unit
                        exchange.flowPropertyFactor = factor
            #If the unit specified do not match with any flow property factor of the flow, the exchange is not created
            if exchange.unit is None:
              log.error('Unit: "' + row[unitCol] + '" cannot be used by flow: "' + exchange.flow.name + '" - ' + exchange.flow.location.code)
            else:
              process.exchanges.add(exchange)
            for e in process.exchanges:
                if e.input == 0:
                    process.quantitativeReference= e
            olca.updateProcess(process)
            processFlows = {}