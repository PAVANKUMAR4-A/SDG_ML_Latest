from SDV_BULK_API_FILE.SDV_BULK_UPDATED_file_API import Bulk_Driver


header = {

"ProcessAreaId": "ENG",

"DataSetName": "TEST_ENG",

"TargetSys": "SDD070",

"CountryKey": "US",

"NumOfRecords": 55,

"CreatedBy" :"T_WIP_USER"

}


body_list = [

{

"FieldName": "PBUKR",

"FieldDesc": "Entity",

"FieldValue": 2200

},
{

"FieldName": "DR",

"FieldDesc": "DefaultRecords",

"FieldValue": 0

},
{

"FieldName": "PLFEZ",

"FieldDesc": "Start Date",

"FieldValue": "2023-06-09"

},
{

"FieldName": "PLSEZ",

"FieldDesc": "End Date",

"FieldValue": "2026-06-08"

},
{

"FieldName": "PST1",

"FieldDesc": "POST1",

"FieldValue": ""

},

{

"FieldName": "MANDT",

"FieldDesc": "MANDT",

"FieldValue": "30"

},
{

"FieldName": "ZZ_CLIENT",

"FieldDesc": "Client",

"FieldValue": 1100016

},
{

"FieldName": "Email",

"FieldDesc": "Requester E-Mail",

"FieldValue": "vamsi.nilla@email.com"

},
{

"FieldName": "PRART",

"FieldDesc": "Engagement Type",

"FieldValue": 1

},

{

"FieldName": "Z3",

"FieldDesc": "Eng Partner",

"FieldValue": 3126673

},
{

"FieldName": "Z4",

"FieldDesc": "Eng Manager",

"FieldValue": 3154886

},
{

"FieldName": "L2_levels",

"FieldDesc": "NoofL2_levels",

"FieldValue": 1

},
{

"FieldName": "M",

"FieldDesc": "Material (L2)",

"FieldValue": "6110-1107"

},
{

"FieldName": "MO",

"FieldDesc": "Market Offering (L2)",

"FieldValue": "AE17063724A0956016"

}

]


input_dictionary = dict()


for i in header.keys():
    input_dictionary[i] = header[i]
    print(i)

print(input_dictionary)



for j in body_list :

    k= j['FieldValue']

    if k == '':
        k= 0

    input_dictionary[j['FieldDesc']] = k

print(input_dictionary)

bulk_object = Bulk_Driver()
response = bulk_object.bulk_driver_method(header, body_list)
print("response after",response)
