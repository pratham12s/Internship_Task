import os.path
import zipfile
import xml.etree.ElementTree as ET
import wget
import csv
import xmltodict
import boto3
from botocore.exceptions import NoCredentialsError
import logging
import unittest

class xmltocsv:

    """

        |    Class xmltocsv :
        | 
        |    which contains methods and variables to convert a xml file to csv and store it in S3 Bucket
        |
        |    Methods :
        |        
        |       parse_xml_file : parses the xml file from the local storage, download the zip file
        |                        with the file type "DLTINS"
        |
        |       unzip_xml_file : Unzips and extracts all XML Files in thr Directoty Named output
        |
        |       convert_xml_to_csv : Parses the XML Files for following Attributes:
        |        
        |                        ->FinInstrmGnlAttrbts.Id
        |                        ->FinInstrmGnlAttrbts.FullNm
        |                        ->FinInstrmGnlAttrbts.ClssfctnTp
        |                        ->FinInstrmGnlAttrbts.CmmdtyDerivInd
        |                        ->FinInstrmGnlAttrbts.NtnlCcy
        |                        ->Issr
        |
        |                        and creates a csv file in the same directory as the initial XML File
        |    
        |       S3_Upload : Uploads the CSV Files created to the S3 Bucket on AWS Cloud

    """ 

    def __init__(self,address):

        
        self.Xml_Address = address
        self.File_Dir, self.file_name = os.path.split(address)
        self.Zip_File_Name = ""
        self.CSV_Name = "output.csv"
        self.File_Type = "DLTINS"
        self.PATH = "output"
        self.all_element = []
        self.unzip_xml_name = []
        self.xml_parsed_doc = []



    
    def parse_xml_file(self):

        """
            |     parse_xml_file() :
            | 
            |     Parses the xml file from the local storage, downloads the zip file 
            |     with the file_type "DLTINS"
            | 
            |     Parameters Used:
            |             Xml_Address(string""): Contains the complete path of the initial XML File
            |     
            |             all_element[List[]]: Contains all the Attributes value dictionary under result tag
            | 
            |             Zip_File_Name(String""): Contains Name of the Downloaded Zip File
            | 
            |             File_Dir(String""): Contains the Directory Name of the XML File Location

        """ 
        try:
            #Reading and Parsing the XML Files.
            tree = ET.parse(self.Xml_Address)
            #Getting the Root of the XML Document.
            root = tree.getroot()
            #Iterating and Finding the Doc Tag.
            levels = root.findall("./result/doc")

            #Iterating the levels to find all the attributes.
            for level in levels:
                xml_info = {}
                #Iterating through all the attributes and storing in a dictionary
                for element in level:
                    xml_info[element.attrib["name"]] = element.text
                self.all_element.append(xml_info)

        except ET.ParseError as err:
            logging.exception(str(err))
        
        except FileNotFoundError as err:
            logging.exception(str(err))

        #Iterating through all the Keys and value and checking if any key has file_type as DLTINS
        for i in range(len(self.all_element)):
            if self.all_element[i]["file_type"] == self.File_Type:                
                try:
                    #Storing and Downloading the File with File_Type as DLTINS and Break
                    self.Zip_File_Name = self.all_element[i]["download_link"].split("/")[-1]
                    wget.download(self.all_element[i]["download_link"],out=self.File_Dir+"/"+self.Zip_File_Name)
                    break

                except Exception as err:
                    logging.exception(str(err))
                
                

        
    
    def unzip_xml_file(self):

        """
            |   unzip_zml_file() :    
            |
            |    Unzips and extracts all XML Files in thr Directoty Named output
            |
            |    Parameters Used:
            |            Zip_File_Name(string""): Contains Name of the Downloaded Zip File
            |
            |            File_Dir(string""): Contains the Directory Name of the XML File Location
            |
            |            unzip_xml_name(List[]): List of XML Files Name Extracted from the Zip File
        
        """ 

        #Iterating through the downloaded zipped file for XML Files
        try:
            with zipfile.ZipFile(self.File_Dir+"/"+self.Zip_File_Name) as zf:
                #Iterating through the Zip folder for all the files
                for member in zf.infolist():
                    #If the item is another directory than continue
                    if member.filename[-1] == "/":
                        continue
                    #Find the Filename from the complete path
                    member.filename = os.path.basename(member.filename)
                    #If the Filename is of type XML than extract it and append its name
                    if member.filename.split(".")[-1] == "xml":
                        zf.extract(member,self.PATH)
                        self.unzip_xml_name.append(member.filename)

        except zipfile.error as err:
            logging.exception(str(err))
        except FileNotFoundError as err:
            logging.exception(str(err))
        except IndexError as err:
            logging.exception(str(err))




    
    def convert_xml_to_csv(self):

        """
            |   convert_xml_to_csv() :
            |
            |    Parses the XML Files for following Attributes:
            |        
            |        ->FinInstrmGnlAttrbts.Id
            |        ->FinInstrmGnlAttrbts.FullNm
            |        ->FinInstrmGnlAttrbts.ClssfctnTp
            |        ->FinInstrmGnlAttrbts.CmmdtyDerivInd
            |        ->FinInstrmGnlAttrbts.NtnlCcy
            |        ->Issr
            |
            |    and creates a csv file in the same directory as the initial XML File
            |
            |    Parameters Used:
            |            File_Dir(string""): Contains the Directory Name of the XML File Location
            |
            |            unzip_xml_name(List[]): List of XML Files Name Extracted from the Zip File
            |
            |            attributes(List[]): List which Contains the attributes needed to be parsed
            |
            |            PATH(string""): Contains the Name of directory containing Unzipped Files
            |
            |            attribute_dict(Dictionary{}): Contains Attribute and its value as dictionary
            |
            |            xml_parsed_doc(List[]): List of attribute_dict,contains all dictionaries 
            |                                    with attribute as key and its text as value
            |
            |            Returns(string""): Path of the Created CSV File    

        """ 

        # Defining all the attributes which needs to be parsed from all the XML Files
        attributes = ["Id", "FullNm", "ClssfctnTp", "CmmdtyDerivInd", "NtnlCcy"]
        # Iterating through the names of all the extracted xml files
        for file in self.unzip_xml_name:
            try:
                # open the XML File  as UTF8
                with open(self.File_Dir+"/"+self.PATH+"/"+file, encoding="utf8") as xml_file:
                    # Convert the XML File to Dictionary 
                    data_dict = xmltodict.parse(xml_file.read())
                    # Storing the list of Dictionary with key as given
                    dict_variable = data_dict["BizData"]['Pyld']['Document']['FinInstrmRptgRefDataDltaRpt']['FinInstrm']
                    # Storing the Length of the List of Dictionary
                    number_of_elements = len(dict_variable)

                # Iterating through the list and getting the dictinary 
                for i in range(number_of_elements):
                    attribute_dict = {}
                    # Creating a dictionary with the required header and corresponding values
                    for attribute in attributes:
                        attribute_dict["FinInstrmGnlAttrbts_"+attribute] = dict_variable[i][list(dict_variable[i].keys())[0]]['FinInstrmGnlAttrbts'][attribute]
                    attribute_dict['Issr'] = dict_variable[i][list(dict_variable[i].keys())[0]]["Issr"]
                    # Adding the one set of dictionary to the list
                    self.xml_parsed_doc.append(attribute_dict)

            except FileNotFoundError as err:
                logging.exception(str(err))
            except IOError as err:
                logging.exception(str(err))
            except IndexError as err:
                logging.exception(str(err))
        # Getting the list of headers for the CSV File
        header_list = list(self.xml_parsed_doc[0].keys())
    
        try:
            # Creating and Opening a CSV File
            with open(self.File_Dir+"/"+self.CSV_Name, 'w',newline='') as csv_file:
                # Creating a Instance of the DictWriter of CSV Package
                writer = csv.DictWriter(csv_file, fieldnames=header_list)
                # Writing the Headers to the CSV File 
                writer.writeheader()
                # Writing each Dictionary as the Row of the CSV File
                writer.writerows(self.xml_parsed_doc)
        except IOError as err:
            logging.exception(str(err))
        # Return the CSV File Name
        return self.File_Dir+"/"+self.CSV_Name 



        
    def S3_Upload(self,source_loc,bucket_name,destination_loc,access_key,secret_key):

        """
            |   S3_Upload() :
            |
            |    Uploads the CSV Files created to the S3 Bucket on AWS Cloud
            |
            |    Parameters Used:
            |
            |            source_loc(String""): Complete path of the CSV File Created
            |
            |            bucket_name(string""): Bucket Name where the CSV File needs to be saved
            |
            |            destination_loc(String""): Complete path of the Location in S3 Bucket
            |
            |            access_key(String""): Contains the Access Key to access the AWS Service
            |
            |            secret key(String""): Contains the Secret Key to access the AWS Service
            |
            |    Returns(Boolean): True (File Uploaded) and False (File Not Uploaded)

        """ 

        # Create an Instance of the Bot3 Module for Uploading the CSV File to S3 Bucket using the Access Key Secret Key
        S3 = boto3.client('s3', aws_access_key_id= access_key, aws_secret_access_key= secret_key)
        try:
            # Upload the file from the local source to the Destination in S3 Bucket with the Bucket Name
            S3.upload_file(source_loc,bucket_name,destination_loc)
            logging.INFO(self.CSV_Name+" Uploaded to S3 Bucket")
        
        except FileNotFoundError as e:
            logging.exception(str(e))

        except NoCredentialsError as e:
            logging.exception(str(e))


class TestingConverion(unittest.TestCase):

    def test_file_address(self):

        # Test creation for the XML Address 
        test_obj = xmltocsv(r"C:\Users\Pratham\Desktop\Internship_Task_2\internship_xml.xml")
        self.assertNotEqual(test_obj.Xml_Address,"","The XML Address Cannot be Empty")
        self.assertNotEqual(test_obj.Xml_Address,None,"The XML Address Cannot be None")
        self.assertEqual(test_obj.Xml_Address,r"C:\Users\Pratham\Desktop\Internship_Task_2\internship_xml.xml",
        "The XML Address Cannot be Empty")

    def test_complete_address(self):
        
        # Test Case creation for the File Directory and the File Name
        test_obj = xmltocsv(r"C:\Users\Pratham\Desktop\Internship_Task_2\internship_xml.xml")
        self.assertEqual(test_obj.File_Dir,r"C:\Users\Pratham\Desktop\Internship_Task_2","Valid Directory Not Found")
        self.assertEqual(test_obj.file_name,r"internship_xml.xml","File Address Not Provided Properly")
    
    def test_all_element(self):

        # Test Case creation for All Element Variable and file type
        test_obj = xmltocsv(r"C:\Users\Pratham\Desktop\Internship_Task_2\internship_xml.xml")
        test_obj.parse_xml_file()
        self.assertEqual(len(test_obj.all_element),4,"All Element Not Calculated Correctly")
        self.assertNotEqual(len(test_obj.all_element),0,"XML Not Parsed Properly")
        self.assertEqual(test_obj.File_Type,"DLTINS","Parsing for the Wrong File Type")
        self.assertNotEqual(test_obj.File_Type,"None","The File Type cannot be None")
    
    def test_unziped_file(self):

        # Test Case for the unzip xml file name
        test_obj = xmltocsv(r"C:\Users\Pratham\Desktop\Internship_Task_2\internship_xml.xml")
        test_obj.parse_xml_file()
        test_obj.unzip_xml_file()
        self.assertNotEqual(len(test_obj.unzip_xml_name),0,"Unzipped File does not have any XML File")
        self.assertEqual(len(test_obj.unzip_xml_name),1,"File Not Unzipped Properly")
    
    def test_parsed_elements(self):

        # Test case for checking the xml_parsed_doc variable, the length of the headers and source location on local file
        test_obj = xmltocsv(r"C:\Users\Pratham\Desktop\Internship_Task_2\internship_xml.xml")
        test_obj.parse_xml_file()
        test_obj.unzip_xml_file()
        source_loc = test_obj.convert_xml_to_csv()
        self.assertNotEqual(len(test_obj.xml_parsed_doc),0,"The XML File Information Cannot Be Empty")
        self.assertNotEqual(len(test_obj.xml_parsed_doc[0].keys()),0,"The Header of the CSV Cannot be empty")
        self.assertEqual(len(test_obj.xml_parsed_doc[0].keys()),6,"The Headers of CSV Not Parsed Correctly")
        self.assertEqual(os.path.exists(source_loc),True,"The CSV File Not Created")
    


if __name__ == "__main__":
    
    # Assign the Values for the Below Variable
    bucket_name = ""
    destination_loc=""
    access_key = ""
    secret_key = ""
    

    # Uncomment to run the Test
    #unittest.main()

    # Uncomment to Convert XML File to CSV and store it to SE Bucket
    obj1 = xmltocsv(r"C:\Users\Pratham\Desktop\Internship_Task_2\internship_xml.xml")
    obj1.parse_xml_file()
    obj1.unzip_xml_file()
    source_loc = obj1.convert_xml_to_csv()
    #result = obj1.S3_Upload(source_loc,bucket_name,destination_loc,access_key,secret_key)






        
    

        

    