from ClusterClient import ClusterClient
import sys
import time
### Below are two large json files randomly generated used for testing basic functions ###
### Scroll to bottom for actual test implementations and function calls ### 
large_json  = [
  {
    "_id": "61423a5f478672d7e02345a4",
    "index": 0,
    "guid": "b944ca27-3c3c-40bb-bcdc-57b432c4a285",
    "isActive": False,
    "balance": "$2,979.11",
    "picture": "http://placehold.it/32x32",
    "age": 33,
    "eyeColor": "green",
    "name": "Tanisha Leon",
    "gender": "female",
    "company": "HANDSHAKE",
    "email": "tanishaleon@handshake.com",
    "phone": "+1 (942) 577-3292",
    "address": "868 Bouck Court, Dante, Georgia, 2133",
    "about": "Lorem in et anim do quis adipisicing. Cillum fugiat proident nostrud in nulla amet elit exercitation id cupidatat officia. Exercitation amet mollit culpa labore officia cillum fugiat Lorem magna cillum sunt do.\r\n",
    "registered": "2016-07-29T07:22:08 +04:00",
    "latitude": -10.319914,
    "longitude": 84.834652,
    "tags": [
      "magna",
      "proident",
      "culpa",
      "deserunt",
      "duis",
      "anim",
      "sit"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Angelina Valencia"
      },
      {
        "id": 1,
        "name": "Aimee Copeland"
      },
      {
        "id": 2,
        "name": "Olson Cole"
      }
    ],
    "greeting": "Hello, Tanisha Leon! You have 8 unread messages.",
    "favoriteFruit": "apple"
  },
  {
    "_id": "61423a5f3d004fd4b59d46fd",
    "index": 1,
    "guid": "8c3303ed-b6d8-47ab-9871-cdfedb36042c",
    "isActive": True,
    "balance": "$2,947.42",
    "picture": "http://placehold.it/32x32",
    "age": 30,
    "eyeColor": "blue",
    "name": "Mayra Trujillo",
    "gender": "female",
    "company": "DIGITALUS",
    "email": "mayratrujillo@digitalus.com",
    "phone": "+1 (870) 408-3390",
    "address": "233 Kenmore Court, Albrightsville, Louisiana, 9098",
    "about": "Cillum commodo ea sint deserunt exercitation culpa incididunt fugiat officia culpa consequat. Sit nostrud exercitation officia sit amet dolor ea est esse ad voluptate consequat. Consectetur consectetur deserunt fugiat culpa in ipsum elit labore velit sint. Excepteur qui duis do aliquip fugiat est anim. Eiusmod eu amet dolore qui sint mollit ad.\r\n",
    "registered": "2018-06-30T05:57:50 +04:00",
    "latitude": 25.007907,
    "longitude": 155.470508,
    "tags": [
      "duis",
      "ullamco",
      "labore",
      "velit",
      "excepteur",
      "eu",
      "pariatur"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Lacey Allison"
      },
      {
        "id": 1,
        "name": "Suzanne Bush"
      },
      {
        "id": 2,
        "name": "Butler Spears"
      }
    ],
    "greeting": "Hello, Mayra Trujillo! You have 7 unread messages.",
    "favoriteFruit": "banana"
  },
  {
    "_id": "61423a5f6d40de34ce8f6ef6",
    "index": 2,
    "guid": "0a42e2d1-3534-4a39-a62c-1765972a5a3f",
    "isActive": True,
    "balance": "$2,533.79",
    "picture": "http://placehold.it/32x32",
    "age": 34,
    "eyeColor": "blue",
    "name": "Burch Abbott",
    "gender": "male",
    "company": "REVERSUS",
    "email": "burchabbott@reversus.com",
    "phone": "+1 (812) 571-3695",
    "address": "489 Thornton Street, Navarre, Tennessee, 9113",
    "about": "Nulla Lorem fugiat proident anim adipisicing duis esse duis sint anim. Cillum nisi eu aute voluptate aliquip eiusmod sit enim sint minim enim ullamco. Incididunt aliquip qui nisi nisi enim magna reprehenderit non fugiat Lorem cupidatat sint.\r\n",
    "registered": "2016-05-24T10:12:59 +04:00",
    "latitude": -57.015338,
    "longitude": -161.144017,
    "tags": [
      "anim",
      "Lorem",
      "deserunt",
      "pariatur",
      "cupidatat",
      "velit",
      "aliquip"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Rosella Hamilton"
      },
      {
        "id": 1,
        "name": "Karin Dudley"
      },
      {
        "id": 2,
        "name": "Nora Ballard"
      }
    ],
    "greeting": "Hello, Burch Abbott! You have 8 unread messages.",
    "favoriteFruit": "banana"
  },
  {
    "_id": "61423a5f22956679df0884bc",
    "index": 3,
    "guid": "41a5efd9-cf44-4556-a8f9-778d18f06739",
    "isActive": False,
    "balance": "$1,897.07",
    "picture": "http://placehold.it/32x32",
    "age": 32,
    "eyeColor": "blue",
    "name": "Porter Jimenez",
    "gender": "male",
    "company": "ECRAZE",
    "email": "porterjimenez@ecraze.com",
    "phone": "+1 (898) 467-3678",
    "address": "214 Ainslie Street, Colton, Wisconsin, 1120",
    "about": "Consectetur eiusmod voluptate amet consectetur irure sint fugiat tempor culpa elit. Incididunt minim proident occaecat ut. Irure nostrud excepteur laboris amet. In occaecat dolore veniam eiusmod ex deserunt aute voluptate sunt magna cupidatat cillum cillum. Anim ea officia nostrud occaecat proident aliqua dolore dolore veniam id aute.\r\n",
    "registered": "2019-09-11T05:29:09 +04:00",
    "latitude": 10.007976,
    "longitude": 22.276935,
    "tags": [
      "aute",
      "esse",
      "do",
      "non",
      "culpa",
      "officia",
      "esse"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Herminia Mcintosh"
      },
      {
        "id": 1,
        "name": "Powers Daniels"
      },
      {
        "id": 2,
        "name": "Lynda Cortez"
      }
    ],
    "greeting": "Hello, Porter Jimenez! You have 3 unread messages.",
    "favoriteFruit": "banana"
  },
  {
    "_id": "61423a5fc3957ddb83c4f853",
    "index": 4,
    "guid": "18a023b0-a2f7-4d52-a8e2-a98214036889",
    "isActive": False,
    "balance": "$1,511.28",
    "picture": "http://placehold.it/32x32",
    "age": 30,
    "eyeColor": "green",
    "name": "Webb Mccormick",
    "gender": "male",
    "company": "ZEAM",
    "email": "webbmccormick@zeam.com",
    "phone": "+1 (863) 549-2171",
    "address": "649 Schenck Street, Charco, Connecticut, 5599",
    "about": "Enim officia dolore labore proident sunt incididunt ullamco elit veniam. Labore ullamco aliqua qui duis adipisicing ex laborum eu voluptate dolor ad. Veniam nulla eu in nostrud nisi do.\r\n",
    "registered": "2015-09-25T10:15:49 +04:00",
    "latitude": 73.409665,
    "longitude": -10.022905,
    "tags": [
      "veniam",
      "in",
      "ut",
      "proident",
      "in",
      "velit",
      "duis"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Rochelle Morton"
      },
      {
        "id": 1,
        "name": "Michelle Alvarado"
      },
      {
        "id": 2,
        "name": "Glenna Russo"
      }
    ],
    "greeting": "Hello, Webb Mccormick! You have 8 unread messages.",
    "favoriteFruit": "apple"
  }
]
large_json_2 = [
  {
    "_id": "61492ab13c64f84c857581e6",
    "index": 0,
    "guid": "3b3768d2-4be7-4971-9210-122e78f6489b",
    "isActive": True,
    "balance": "$1,009.80",
    "picture": "http://placehold.it/32x32",
    "age": 25,
    "eyeColor": "green",
    "name": "Jefferson Lambert",
    "gender": "male",
    "company": "COMCUR",
    "email": "jeffersonlambert@comcur.com",
    "phone": "+1 (829) 486-3346",
    "address": "615 Karweg Place, Bourg, Puerto Rico, 5193",
    "about": "Nisi anim duis proident reprehenderit anim non sunt eiusmod voluptate. Voluptate labore excepteur esse cupidatat minim non consequat cupidatat sint in et. Eiusmod in enim esse cupidatat proident. Quis duis pariatur proident pariatur amet ullamco ea. Cillum officia irure commodo ut ex adipisicing quis adipisicing commodo labore do id nulla laborum. Aute adipisicing sunt excepteur amet ipsum pariatur ullamco amet fugiat incididunt quis id aute. Eu consequat aliqua aliquip irure incididunt sunt.\r\n",
    "registered": "2018-11-16T05:52:28 +05:00",
    "latitude": -21.547408,
    "longitude": 160.501363,
    "tags": [
      "minim",
      "do",
      "occaecat",
      "veniam",
      "ea",
      "adipisicing",
      "elit"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Mcmahon Barton"
      },
      {
        "id": 1,
        "name": "Shepherd Fox"
      },
      {
        "id": 2,
        "name": "Carroll Howard"
      }
    ],
    "greeting": "Hello, Jefferson Lambert! You have 6 unread messages.",
    "favoriteFruit": "strawberry"
  },
  {
    "_id": "61492ab1e7f6d1ff16d99c50",
    "index": 1,
    "guid": "6dcf833c-9d53-4a78-ae0e-75164909f250",
    "isActive": False,
    "balance": "$3,789.13",
    "picture": "http://placehold.it/32x32",
    "age": 27,
    "eyeColor": "green",
    "name": "Brooke Gates",
    "gender": "female",
    "company": "COMTOUR",
    "email": "brookegates@comtour.com",
    "phone": "+1 (870) 483-2709",
    "address": "749 Dekoven Court, Elrama, South Carolina, 4770",
    "about": "Qui eu voluptate labore amet ea sit veniam et enim in sunt. Reprehenderit labore amet minim aliquip eiusmod esse. Laborum proident et pariatur laborum nulla consequat laboris exercitation incididunt. Excepteur reprehenderit mollit magna et proident aliqua consectetur aliquip quis esse sit et sunt.\r\n",
    "registered": "2021-02-11T07:33:12 +05:00",
    "latitude": -13.215941,
    "longitude": -72.340232,
    "tags": [
      "ex",
      "consequat",
      "Lorem",
      "sint",
      "minim",
      "enim",
      "labore"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Claudine Miranda"
      },
      {
        "id": 1,
        "name": "Bush Forbes"
      },
      {
        "id": 2,
        "name": "Angela Carrillo"
      }
    ],
    "greeting": "Hello, Brooke Gates! You have 2 unread messages.",
    "favoriteFruit": "strawberry"
  },
  {
    "_id": "61492ab174cdbe35ee1c18cc",
    "index": 2,
    "guid": "6d77ac1f-aead-43a4-b896-fd2b9a3700b8",
    "isActive": True,
    "balance": "$2,246.18",
    "picture": "http://placehold.it/32x32",
    "age": 38,
    "eyeColor": "blue",
    "name": "Sophie Chandler",
    "gender": "female",
    "company": "DAIDO",
    "email": "sophiechandler@daido.com",
    "phone": "+1 (870) 403-3667",
    "address": "806 Dewey Place, Gambrills, Northern Mariana Islands, 6285",
    "about": "Non ea dolor Lorem enim ut mollit magna voluptate consectetur commodo commodo. Esse cupidatat est ad laboris sit laboris ad consectetur ullamco laboris. Sunt do sint laborum et eiusmod dolor occaecat excepteur minim ut veniam irure. Enim esse Lorem nulla in ex dolor. Laboris cupidatat aliquip incididunt veniam ex tempor do adipisicing nostrud.\r\n",
    "registered": "2014-09-09T11:08:01 +04:00",
    "latitude": 80.214143,
    "longitude": 4.345966,
    "tags": [
      "irure",
      "mollit",
      "esse",
      "eu",
      "occaecat",
      "cupidatat",
      "duis"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Lorrie Beasley"
      },
      {
        "id": 1,
        "name": "Lamb Roman"
      },
      {
        "id": 2,
        "name": "Santana Hanson"
      }
    ],
    "greeting": "Hello, Sophie Chandler! You have 5 unread messages.",
    "favoriteFruit": "strawberry"
  },
  {
    "_id": "61492ab17407b12b3036c05f",
    "index": 3,
    "guid": "f47f59de-1621-4891-a5de-e2057960c9a7",
    "isActive": False,
    "balance": "$2,266.82",
    "picture": "http://placehold.it/32x32",
    "age": 32,
    "eyeColor": "green",
    "name": "Chris Kerr",
    "gender": "female",
    "company": "TYPHONICA",
    "email": "chriskerr@typhonica.com",
    "phone": "+1 (833) 563-3354",
    "address": "741 Johnson Street, Makena, Arizona, 2214",
    "about": "Ad reprehenderit in officia incididunt ex enim quis ex aliquip nostrud. Cupidatat Lorem et nisi commodo consectetur do reprehenderit eiusmod cillum deserunt qui occaecat amet. Aute duis incididunt occaecat amet. Sit duis sint do qui ipsum ut sint fugiat anim quis. Lorem proident Lorem cillum minim ex ad incididunt mollit fugiat consequat eiusmod in esse. Aute irure laborum sint exercitation nulla dolore pariatur amet eiusmod qui.\r\n",
    "registered": "2014-11-06T04:08:11 +05:00",
    "latitude": -18.217295,
    "longitude": 50.031216,
    "tags": [
      "irure",
      "minim",
      "deserunt",
      "velit",
      "aliquip",
      "duis",
      "exercitation"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Jewell Wall"
      },
      {
        "id": 1,
        "name": "Perry Hardy"
      },
      {
        "id": 2,
        "name": "Valeria Cook"
      }
    ],
    "greeting": "Hello, Chris Kerr! You have 9 unread messages.",
    "favoriteFruit": "apple"
  },
  {
    "_id": "61492ab1eb3bf6b0c11ae6b9",
    "index": 4,
    "guid": "9a2bac2a-a40f-42f4-9bc7-06f43d37d370",
    "isActive": False,
    "balance": "$1,233.21",
    "picture": "http://placehold.it/32x32",
    "age": 39,
    "eyeColor": "brown",
    "name": "Foley David",
    "gender": "male",
    "company": "ANDERSHUN",
    "email": "foleydavid@andershun.com",
    "phone": "+1 (876) 572-2204",
    "address": "811 Clifford Place, Sanders, Massachusetts, 9014",
    "about": "Enim minim id reprehenderit aute. Magna aliquip labore ad excepteur laboris qui laborum do fugiat non. Lorem ipsum aute labore elit excepteur quis do est reprehenderit quis aute. Ullamco duis laboris ex nostrud eiusmod nisi culpa quis qui dolore labore reprehenderit tempor.\r\n",
    "registered": "2014-11-25T04:34:20 +05:00",
    "latitude": 70.507993,
    "longitude": 72.304615,
    "tags": [
      "culpa",
      "reprehenderit",
      "dolor",
      "sunt",
      "cillum",
      "sunt",
      "ipsum"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Blackwell Garcia"
      },
      {
        "id": 1,
        "name": "Beth Lyons"
      },
      {
        "id": 2,
        "name": "Hunter Bonner"
      }
    ],
    "greeting": "Hello, Foley David! You have 4 unread messages.",
    "favoriteFruit": "strawberry"
  },
  {
    "_id": "61492ab1894b7680cd719795",
    "index": 5,
    "guid": "c1a150e5-3a3e-454c-87ed-208f0f132198",
    "isActive": True,
    "balance": "$1,039.24",
    "picture": "http://placehold.it/32x32",
    "age": 22,
    "eyeColor": "brown",
    "name": "Candice Reilly",
    "gender": "female",
    "company": "ZOSIS",
    "email": "candicereilly@zosis.com",
    "phone": "+1 (918) 491-3151",
    "address": "207 Prospect Place, Kennedyville, Washington, 9188",
    "about": "Ad ullamco elit cupidatat sunt fugiat. Tempor laboris amet consequat occaecat exercitation minim qui officia. Ullamco dolore enim deserunt et commodo aute pariatur qui laboris nisi adipisicing. Mollit reprehenderit excepteur deserunt culpa laborum voluptate eu nostrud aute. Dolore est deserunt occaecat proident ea dolore occaecat do excepteur veniam ex. Ad voluptate velit ad incididunt proident ex dolor elit reprehenderit culpa tempor. Ea nisi velit duis consectetur consectetur aliqua veniam esse elit Lorem ullamco qui Lorem minim.\r\n",
    "registered": "2020-03-26T04:50:32 +04:00",
    "latitude": -4.274285,
    "longitude": -155.142866,
    "tags": [
      "anim",
      "ad",
      "consequat",
      "occaecat",
      "do",
      "id",
      "officia"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Hammond Bernard"
      },
      {
        "id": 1,
        "name": "Bonner Gould"
      },
      {
        "id": 2,
        "name": "Gina Vincent"
      }
    ],
    "greeting": "Hello, Candice Reilly! You have 8 unread messages.",
    "favoriteFruit": "strawberry"
  },
  {
    "_id": "61492ab1b0dd1ce76085536b",
    "index": 6,
    "guid": "2bbe6737-7cd4-4c17-8b12-433ab0a1f841",
    "isActive": True,
    "balance": "$3,833.96",
    "picture": "http://placehold.it/32x32",
    "age": 29,
    "eyeColor": "blue",
    "name": "Burt Burgess",
    "gender": "male",
    "company": "BOINK",
    "email": "burtburgess@boink.com",
    "phone": "+1 (987) 538-3537",
    "address": "213 Banner Avenue, Gilgo, California, 8485",
    "about": "Labore Lorem elit anim reprehenderit adipisicing nisi sit nisi sit. Nostrud ad duis cupidatat cupidatat nostrud id dolor eu consectetur. Fugiat sunt deserunt pariatur proident tempor. Et elit minim anim id cillum ullamco. Dolore nostrud excepteur eiusmod minim esse ullamco culpa sit laborum amet tempor anim consectetur pariatur.\r\n",
    "registered": "2014-10-14T02:35:55 +04:00",
    "latitude": 71.375091,
    "longitude": -57.432303,
    "tags": [
      "est",
      "commodo",
      "pariatur",
      "esse",
      "voluptate",
      "incididunt",
      "aliquip"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Davenport Shepard"
      },
      {
        "id": 1,
        "name": "Yesenia Hewitt"
      },
      {
        "id": 2,
        "name": "Clara Cleveland"
      }
    ],
    "greeting": "Hello, Burt Burgess! You have 4 unread messages.",
    "favoriteFruit": "banana"
  }
]

small_json = {
"name":"fred",
"tall":"6feet",
"features":{"hair":"brown","eye":"brown"}
}

# Test Basics:
client = ClusterClient(str(sys.argv[1]),int(sys.argv[2]),int(sys.argv[3]))

##testing insert with large and small jsons of different sizes
print("Testing Insert:")
print()
client.insert("test1",large_json)
client.insert("test2",small_json)
client.insert("test3",small_json)
###testing insert in loop
for i in range(5):
    client.insert("test_"+str(i),small_json)
print()

###testing remove from above inserts
print("Testing Remove:")
print()
client.remove("test1")
client.remove("test2")
client.remove("test3")
print()

###testing lookup from above inserts
print("Testing Lookup:")
print()
client.lookup("test_1")
client.lookup("test_2")
client.lookup("test_3")
print()

###testing lookup with non existent keys
print("Lookup Test W/O Keys")
print()
client.lookup("test1")
client.lookup("test2")
print()

##testing remove with non existent keys
print("Remove Test W/o Keys")
print()
client.remove("test1")
client.remove("test2")
print()


###testing scan
print("Testing Scan Method")
print()
client.scan("test")
for i in range(5):
    client.insert("scan_test_2_"+str(i),large_json_2)
client.scan("test")
print()

##testing scan without appropriate regex key
print("Testing Scan Method W/O Keys")
print()
client.scan("k")
print()
#time.sleep(1)
##testinng scan again with appropriate regex
print("Testing Scan Method again")
#print()
client.scan("te")
#print()

##testing different arbitrary json
print("Testing with different arbitrary json")
print() 
arb_value = {
"gender" : "male",
"type" : "zebra",
"attribute" : { "weight":100, "skills":{"speed":"100","strength":"250"}},
"location":"africa"
}
for i in range(5):
     client.insert("arb_test_"+str(i),arb_value)
     client.remove("arb_test_"+str(i))
print()
print("Tests finished")
