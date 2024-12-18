import glob
from typing import List
from fastapi import APIRouter, HTTPException, Query
import os
import json 
import duckdb
from app.handler.local_activities import save_json
import pandas as pd
import re

router = APIRouter(prefix="/api")

title_nepali_to_english = {
    'ह्युमपाइप खरिद': 'Purchase of Humepipe',
    'स्वास्थ्य चौकि भवन मर्मत सुधार': 'Health Post Building Repair and Maintenance',
    'स्थानीय स्तर विपद् पूर्व तयारी तथा प्रतिकार्यकार्यक्रम': 'Local Level Disaster Preparedness and Response Program',
    'स्तनपान प्रवर्ध्दन कार्यक्रम': 'Breastfeeding Promotion Program',
    'सेवा र परामर्श खर्च': 'Service and Consulting Expenses',
    'सूचनाप्रवाह प्रणालीको प्रयोग': 'Use of Information Flow System',
    'सूचना प्रणाली तथा सफ्टवेयर संचालन खर्च': 'Information System and Software Operation Costs',
    'सीप विकास तथा जनचेतना तालिम तथा गोष्ठी सम्बन्धी खर्च': 'Skill Development and Public Awareness Training and Seminar Expenses',
    'सांस्कृतिक सम्पदा संरक्षण कार्यक्रम': 'Cultural Heritage Conservation Program',
    'सार्वजनिक भवन निर्माण': 'Public Building Construction',
    'सामान्य औषधि खरिद/ढुवानी': 'General Medicine Purchase/Transport',
    'सामाजिक सुरक्षा तथा संरक्षण': 'Social Security and Protection',
    'सामाजिक सहयता': 'Social Assistance',
    'सामाजिक समावेशीकरण': 'Social Inclusion',
    'सामाजिक सदभाव तथा एकिकरण प्रवद्र्धन कार्यक्रम': 'Social Harmony and Integration Enhancement Program',
    'सहकारी संस्थाको प्रबर्द्धन र विकास कार्यक्रम': 'Cooperative Institutions Promotion and Development Program',
    'सहकारी तालीम कार्यक्रम': 'Cooperative Training Program',
    'संस्थागत कार्य जिम्मेवारी विभाजन': 'Institutional Work Responsibility Division',
    'सवारी साधन मर्मत खर्च': 'Vehicle Repair Expenses',
    'सवारी साधन तथा मेशिनर औजार भाडा': 'Vehicle and Machinery Equipment Rental',
    'सवारी साधन': 'Vehicles',
    'सरसफाई कार्यक्रम': 'Cleaning Program',
    'संरक्षण': 'Conservation',
    'सरकारी भवन/सार्वजनिक स्थल मर्मत सम्भार': 'Government/Public Places Building Maintenance',
    'समुदायमा आधारीत वालरोग उपचार व्यवस्थापन': 'Community-Based Child Disease Treatment Management',
    'सभा सञ्चालन खर्च': 'Meeting Operation Expenses',
    'सडक स्तरोन्नती कार्य': 'Road Improvement Work',
    'सडक निर्माण र स्तरोन्नति तथा मर्मत कार्यक्रम': 'Road Construction, Improvement, and Maintenance Program',
    'सडक निर्माण तथा ग्राभेल कार्य': 'Road Construction and Graveling Work',
    'सडक निर्माण': 'Road Construction',
    'संचार महसुल': 'Communication Revenue',
    'श्रोत संरक्षण': 'Resource Conservation',
    'शैक्षिक पूर्वाधार निर्माण कार्यक्रम': 'Educational Infrastructure Construction Program',
    'शहरी विकास कार्यक्रम': 'Urban Development Program',
    'व्यापार प्रवद्र्धन कार्यक्रम': 'Trade Promotion Program',
    'वैकल्पिक ऊर्जा विकास एवं प्रवर्द्धन कार्यक्रम': 'Alternative Energy Development and Enhancement Program',
    'विविध खर्च': 'Miscellaneous Expenses',
    'विपद् पूर्व तयारी, खोज तथा उद्धार, राहत सामग्रीको पूर्व भण्डारणकार्यक्रम': 'Disaster Preparedness, Search and Rescue, Pre-Storage of Relief Materials Program',
    'विद्युत वितरण प्रणालीको विस्तार कार्यक्रम': 'Electricity Distribution System Expansion Program',
    'विद्यालय शिक्षा तथा पोषण कार्यक्रम': 'School Education and Nutrition Program',
    'विद्यालय भवन मर्मत': 'School Building Repair',
    'विद्यालय आयुर्वेद कार्यक्रम': 'School Ayurveda Program',
    'विद्यार्थी सिकाई उपलब्धी परीक्षण र व्यवस्थापन कार्यक्रम': 'Student Learning Achievement Testing and Management Program',
    'विद्यार्थी प्रोत्साहन तथा छात्रवृत्तिकार्यक्रम': 'Student Encouragement and Scholarship Program',
    'वातावरणीय जोखिम न्यूनीकरण कार्यक्रम': 'Environmental Risk Reduction Program',
    'वातावरण संरक्षण कार्यक्रम': 'Environmental Conservation Program',
    'लघु तथा साना सिचाई कार्यक्रम': 'Small and Micro Irrigation Program',
    'लघु तथा साना जलविद्यत उत्पादन, स्तरोन्नति तथा मर्मत कार्यक्रम': 'Small and Micro Hydropower Production, Upgradation and Maintenance Program',
    'रोकथाम मुलक कार्यक्रम': 'Preventive Measures Program',
    'योजना तर्जुमा र कार्यन्वयन': 'Plan Translation and Implementation',
    'युवा तथा किशोरकिशोरी स्वास्थ्य सम्बन्धी कार्यक्रम': 'Youth and Adolescent Health Program',
    'मेशिनरी तथा औजार मर्मत सम्भार तथा सञ्चालन खर्च': 'Machinery and Tool Repair, Maintenance and Operation Expenses',
    'मेशिनरी तथा औजार': 'Machinery and Tools',
    'माध्यमिक शिक्षाकार्यक्रम': 'Secondary Education Program',
    'महिला स्वास्थ्य स्वयं सेविका भत्ता': 'Women\'s Health Volunteer Allowance',
    'महिला स्वास्थ्य स्वयं सेविका तालिम': 'Women\'s Health Volunteer Training',
    'महिला सशक्तिकरण तथा उत्थान कार्यक्रम': 'Women\'s Empowerment and Upliftment Program',
    'महिला बेचविखन तथा हिंसा नियन्त्रण कार्यक्रम': 'Women Trafficking and Violence Control Program',
    'महंगी भत्ता': 'Cost of Living Allowance',
    'मसलन्द तथा कार्यालय सामाग्री': 'Furniture and Office Supplies',
    'मन्दिर तथा चर्च व्यवस्थापन कार्यक्रम': 'Temple and Church Management Program',
    'मत्स्य विकास कार्यक्रम': 'Fisheries Development Program',
    'भ्रमण खर्च': 'Travel Expenses',
    'भूमिगत सिंचाई निर्माण र स्तरोन्नति तथा मर्मत कार्यक्रम': 'Underground Irrigation Construction, Improvement, and Maintenance Program',
    'भवन निर्माण, तारबार तथा मर्मत': 'Building Construction, Fencing and Repair',
    'भवन निर्माण': 'Building Construction',
    'बीउ ंप्रवद्र्धन तथा गुण नियन्त्रण कार्यक्रम': 'Seed Enhancement and Quality Control Program',
    'बिमा तथा नवीकरण खर्च': 'Insurance and Renewal Expenses',
    'बिबिध': 'Miscellaneous',
    'बाली विकास तथा संरक्षण कार्यक्रम': 'Crop Development and Conservation Program',
    'बालबालिका सम्बन्धी कार्यक्रम': 'Children\'s Programs',
    'बाढी भविष्यवाण्ी कार्यक्रम': 'Flood Forecasting Program',
    'बागवानी विकास कार्यक्रम': 'Horticulture Development Program',
    'बस्ती तथा आवास व्यवस्था': 'Settlement and Housing Arrangement',
    'बन्ध्याकरण सम्बन्धी कार्यक्रम': 'Sterilization Program',
    'फोहरमैला व्यवस्थापनअभिमूखीकरण कार्यक्रम': 'Waste Management Orientation Program',
    'फिल्ड भत्ता': 'Field Allowance',
    'फिल्ड निर्माण': 'Field Construction',
    'फर्निचर तथा फिक्चर्स': 'Furniture and Fixtures',
    'प्राविधिक शिक्षा तथा व्यावसायिक तालिमकार्यक्रम': 'Technical Education and Vocational Training Program',
    'प्रारम्भिक बाल शिक्षा तथा विद्यालय शिक्षाकार्यक्रम': 'Early Childhood and School Education Program',
    'प्राथमिक अस्पताल (१५ वेड)': 'Primary Hospital (15 Beds)',
    'प्रसूती पश्चातको सेवा': 'Postpartum Services',
    'पोशाक': 'Clothing',
    'पुल तथा पुलेसा निर्माण एबं मर्मत कार्यक्रम': 'Bridge and Culvert Construction and Repair Program',
    'पुरातत्व, प्राचीन स्मारक र संग्रहालयको संरक्षण, सम्भार, प्रवर्द्धन र विकासकार्यक्रम': 'Conservation, Maintenance, Promotion, and Development of Archaeology, Ancient Monuments, and Museums',
    'पारिश्रमिक कर्मचारी': 'Salaried Employee',
    'पानी मुहान संरक्षण कार्यक्रम': 'Water Source Conservation Program',
    'पानी तथा बिजुली': 'Water and Electricity',
    'पाठ्यमक्रम र पाठ्य्रसामग्री वितरणकार्यक्रम': 'Curriculum and Educational Material Distribution Program',
    'पशुपन्छी बजार प्रवद्र्धन कार्यक्रम': 'Livestock Market Promotion Program',
    'पशुपंक्षी उत्पादन तथ्याङ्क अभिलेखीकरण': 'Livestock Production Data Recording',
    'पशु विकास सेवा कार्यक्रम': 'Animal Development Service Program',
    'पशु रोग पहिचान तथा नियन्त्रण कार्यक्रम': 'Animal Disease Identification and Control Program',
    'पर्यटन पूर्वाधार विकास कार्यक्रम': 'Tourism Infrastructure Development Program',
    'परिवार नियोजन सम्बन्धी तालिम': 'Family Planning Related Training',
    'परिवार नियोजन र अन्य सेवाको एकिकरण': 'Integration of Family Planning and Other Services',
    'परामर्श सेवा': 'Consultation Services',
    'परम्परागरत जात्रा, पर्वहरुको सञ्चालन र व्यवस्थापनकार्यक्रम': 'Traditional Festivals and Events Management and Operation Program',
    'पदाधिकारीअन्य सुबिधा': 'Official Other Facilities',
    'पदाधिकारी बैठक भत्ता': 'Official Meeting Allowance',
    'पत्रपत्रिका, छपाई तथा सूचना प्रकाशन खर्च': 'Newspaper, Printing and Information Publication Expenses',
    'पञ्जिकरणअभिलेखीकरण': 'Registration and Documentation',
    'निःशुल्क स्वास्थ्य औषधि खरिद/ढुवानी': 'Free Health Medicine Purchase/Transport',
    'निर्मित भवनको संरचना सुधार तथा मर्मत': 'Constructed Building Structural Improvement and Repair',
    'नियमित खोप संचालन': 'Regular Vaccination Program',
    'नियमित अनुगमन': 'Regular Monitoring',
    'नारी दिवस': 'Women\'s Day',
    'नवजात शिशु स्याहार कार्यक्रम': 'Newborn Care Program',
    'तलब भत्ता': 'Salary Allowance',
    'डट्स सम्बन्धी कार्यक्रम': 'DOTS Related Program',
    'झोलुंगे पुल निर्माण र स्तरोन्नति तथा मर्मत कार्यक्रम': 'Suspension Bridge Construction, Upgradation and Maintenance Program',
    'जेष्ठ नागरिक सम्मान कार्यक्रम': 'Senior Citizen Honor Program',
    'घरभाडा': 'House Rent',
    'गरिबी निवारण': 'Poverty Alleviation',
    'खोप सेवा संचालन': 'Vaccination Service Operation',
    'खेलकूद': 'Sports',
    'खेलकुद सामाग्री खरिद': 'Sports Equipment Purchase',
    'खेलकुद कार्यक्रम संचालन': 'Sports Program Operation',
    'खुला तथा वैकल्पिक ९गुरुकुल, मदरसा, गुम्वा आदि० एवम् निरन्तर सिकाइ तथा विशेष शिक्षा कार्यक्रम': 'Open and Alternative (Gurukul, Madrasa, Gumba etc.) Continuous Learning and Special Education Programs',
    'खानेपानी निर्माण': 'Drinking Water Construction',
    'खानेपानी आयोजना सम्बन्धी कार्यक्रम': 'Drinking Water Project Related Program',
    'खानेपानी आपूर्ति कार्यक्रम': 'Drinking Water Supply Program',
    'क्षयरोगीको खोज पड्ताल': 'Tuberculosis Patient Search',
    'कृषि सूचना तथा संचार कार्यक्रम': 'Agricultural Information and Communication Program',
    'कृषि व्यवसाय पवद्र्धन तथा बजार विकास कार्यकैम': 'Agricultural Business Enhancement and Market Development Program',
    'कृषि प्रसार तथा तालीम कार्यक्रम': 'Agricultural Extension and Training Program',
    'कृषि उत्पादन तथ्याङ्क अभिलेखीकरण': 'Agricultural Production Data Recording',
    'कुलो निर्माण कार्य': 'Canal Construction Work',
    'कार्यालयको लागि फर्निचर तथा फिक्चर्स': 'Office Furniture and Fixtures',
    'कार्यालय सञ्चालन तथा प्रशासनिक': 'Office Operation and Administration',
    'कार्यालय भवन निर्माण': 'Office Building Construction',
    'कार्यक्रम सामान/वस्तु ढुवानी': 'Program Goods/Materials Transportation',
    'कार्यक्रम खर्च': 'Program Expenses',
    'कर्मचारीको योगदानमा आधारित बीमा कोष खर्च': 'Employee Contribution Based Insurance Fund Expenses',
    'कर्मचारीको योगदानमा आधारित निवृतभरण तथा उपदान कोष खर्च': 'Employee Contribution Based Retirement and Grant Fund Expenses',
    'कर्मचारीको बैठक भत्ता': 'Employee Meeting Allowance',
    'कर्मचारी प्रोत्साहन तथा पुरस्कार': 'Employee Incentive and Awards',
    'कर्मचारी प्रशिक्षण': 'Employee Training',
    'कर्मचारी कल्याण कोष': 'Employee Welfare Fund',
    'करार सेवा शुल्क': 'Contract Service Fee',
    'औषधी, औजार तथा उपकरण खरिद': 'Medicine, Tools and Equipment Purchase',
    'उन्नत बीउ विजन खरिद तथा वितरण': 'Advanced Seed Vision Purchase and Distribution',
    'उद्यमशीलता विकास कार्यक्रम': 'Entrepreneurship Development Program',
    'इन्धन (पदाधिकारी)': 'Fuel (Officials)',
    'इन्धन (कार्यालय प्रयोजन)': 'Fuel (Office Use)',
    'इन्धन - अन्य प्रयोजन': 'Fuel - Other Purposes',
    'इन्टेक निर्माण': 'Intake Construction',
    'आयमूलक शीप विकास कार्यक्रम': 'Income Generating Skill Development Program',
    'आयआर्जन कार्यक्रम': 'Income Generation Program',
    'आमा सुरक्षा कार्यक्रम': 'Mother Protection Program',
    'आधारभूत तह शिक्षा कार्यक्रम': 'Basic Level Education Program',
    'अस्पताल जन्य फर्निचर तथा फिक्चर्स': 'Hospital-Related Furniture and Fixtures',
    'अपाङ्ग सम्बन्धी कार्यक्रम': 'Disability Related Programs',
    'अन्य सार्वजनिक निर्माण': 'Other Public Construction',
    'अन्य सामाजिक सुरक्षा खर्च': 'Other Social Security Expenses',
    'अन्य मर्मत सूधार': 'Other Repair and Maintenance',
    'अन्य भाडा': 'Other Rentals',
    'अन्य फिर्ता': 'Other Returns',
    'अन्य पूर्वाधार निर्माण': 'Other Infrastructure Construction',
    'अन्य निर्माण': 'Other Construction',
    'अन्य कार्यालय संचालन खर्च': 'Other Office Operating Expenses',
    'अन्य कार्यक्रम': 'Other Programs',
    'अन्य': 'Miscellaneous',
    'अनौपचारिक शिक्षाकार्यक्रम': 'Informal Education Programs',
    'अनुगमन, मूल्यांकन खर्च': 'Monitoring, Evaluation Expenses',
    'अनुगमन तथा मूल्यांकन': 'Monitoring and Evaluation',
    'ग्रामीण सडक निर्माण र स्तरोन्नति तथा मर्मत कार्यक्रम': 'Rural Road Construction, Improvement and Maintenance Program',
    "अनुदानमा आधारित युवा लक्षित कार्यक्रम": "Youth-targeted Program Based on Grants",
    "फलफुल विकास कार्यक्रम": "Fruit Development Program",
    "विरुवा वितरण": "Seedling Distribution",
    "५० प्रतिशत अनुदान": "50 Percent Grant",
    "नयाँ लघुउद्यमी विकास कार्यक्रम": "New Small Enterprise Development Program",
    "बजार व्यवस्थापन कार्यक्रम": "Market Management Program",
    "कृषि सामाग्री खरिद": "Agricultural Materials Purchase",
    "लघु, घरेलु तथा साना उद्योग नियमन तथा प्रवद्र्धन कार्यक्रम": "Regulation and Promotion of Small, Household and Micro Industries",
    "निर्माण": "Construction",
    "बीउ विजन खरिद तथा वितरण": "Seed Vision Purchase and Distribution",
    "मेशिनरी औजार वितरण": "Machinery Tools Distribution",
    "नालि": "Drainage",
    "कुलो मर्मत कार्य": "Canal Repair Work",
    "सिँचाई पाईप खरिद": "Irrigation Pipe Purchase",
    "सिँचाई मर्मत": "Irrigation Repair",
    "सिचार्इ पोखरी निर्माण": "Irrigation Pond Construction",
    "सिचार्इ पोखरी मर्मत": "Irrigation Pond Repair",
    "रोग नियन्त्रण कार्यक्रम": "Disease Control Program",
    "अन्य खरिद कार्य": "Other Purchasing Activities",
    "भूसंरक्षण कार्यक्रम": "Soil Conservation Program",
    "निर्माण तथा मर्मत": "Construction and Repair",
    "शैक्षिक सामाग्री खरिद": "Educational Materials Purchase",
    "क्षमता विकास तालिम": "Capacity Development Training",
    "फर्निचर खरिद, निर्माण तथा मर्मत कार्य": "Furniture Purchase, Construction, and Repair",
    "फर्निचर निर्माण तथा खरिद कार्य": "Furniture Construction and Purchase",
    "फर्निचर निर्माण तथा मर्मत कार्य": "Furniture Construction and Repair",
    "फिल्ड निर्माण तथा तारबार": "Field Construction and Fencing",
    "विद्यालय मर्मत सुधार": "School Repair and Improvement",
    "छात्रवृत्ति": "Scholarship",
    "अन्य केन्द्र": "Other Centers",
    "अनुदान": "Grant",
    "औषधियुक्त जडिबुटीको प्रयोग": "Use of Medicinal Herbs",
    "महिला स्वास्थ्य स्वयम्सेविका तालिम": "Female Health Volunteer Training",
    "विविध": "Miscellaneous",
    "स्वास्थ्य सचेतना कार्यक्रम": "Health Awareness Program",
    "खोप अभियान सञ्चालन गर्ने": "Conduct Vaccination Campaign",
    "स्वास्थ्य सेवाको गुणस्तर मापन": "Quality Measurement of Health Services",
    "जनशक्ति उत्पादन र व्यवस्थापन": "Manpower Production and Management",
    "रेबिज रोग तथा रेबिज भ्याक्सिन प्रयोगसम्बन्धि कार्यक्रम": "Rabies Disease and Vaccine Use Program",
    "पोषणयुक्त आहार निर्माण": "Nutritious Food Production",
    "गर्भवती महिलालाई आइरन चक्की र जुकाको औषधि वितरण": "Distribution of Iron Tablets and Anti-Worm Medication to Pregnant Women",
    "पोषण सम्बन्धी विविध कार्यक्रम": "Various Nutrition Related Programs",
    "खरिद कार्य": "Purchasing Activity",
    "स्वास्थ्य चौकि टाइप डि": "Type D Health Post",
    "मर्मत संभार": "Repair and Maintenance",
    "खानेपानी टंङ्की निर्माण": "Water Tank Construction",
    "खानेपानी सम्बन्धी योजना": "Water Supply Plan",
    "पाईप खरिद": "Pipe Purchase",
    "फोहरमैला व्यवस्थापन कार्यक्रम": "Waste Management Program",
    "खानेपानी पार्इप खरिद तथा इन्टेक निर्माण": "Water Pipe Purchase and Intake Construction",
    "खानेपानी मर्मत": "Water System Repair",
    "मन्दिर, चर्च निर्माण तथा मर्मत": "Temple, Church Construction and Repair",
    "मठ तथा मन्दिर मर्मत": "Monastery and Temple Repair",
    "अपाङ्गता सम्बन्धि विविध कार्यक्रम": "Various Programs Related to Disabilities",
    "सम्मान": "Honor",
    "बालमैत्री कार्यक्रम": "Child Friendly Program",
    "लक्षित कार्यक्रम संचालन": "Targeted Program Operation",
    "एकल महिला सम्बन्धि कार्यक्रम": "Single Women Related Program",
    "प्रवर्द्धनात्मक कार्यक्रम": "Promotional Program",
    "निर्माण": "Construction",
    "नविकरणिय उर्जा": "Renewable Energy",
    "लार्इन विस्तार": "Line Extension",
    "सम्पदा पूर्वाधार": "Heritage Infrastructure",
    "पुननिर्माण": "Reconstruction",
    "भैपरी आउने पूँजीगत": "Emergency Capital",
    "मर्मत संभार कोष": "Repair and Maintenance Fund",
    "विपद् व्यवस्थापन": "Disaster Management",
    "कर्मचारी वृत्ति विकास कार्यक्रम": "Employee Scholarship Development Program",
    "मानव संसाधन विकासयोजना तर्जुमा तथा कार्यान्वयन": "Translation and Implementation of Human Resource Development Plan",
    "कानुन तथा न्याय": "Law and Justice",
    "शासन प्रणाली": "Governance System",
    "नतीजामा आधारित कार्यसम्पादन मूल्यांकन": "Performance Evaluation Based on Results",
    "अन्य सम्पत्तिहरूको संचालन तथा सम्भार खर्च": "Expenses for Operation and Maintenance of Other Properties",
    "अन्य सेवा शुल्क": "Other Service Fees",
    "अन्य भत्ता": "Other Allowance",
    "अन्य भ्रमण खर्च": "Other Travel Expenses",
    "धार्मिक तथा सांस्कृतिक संस्था सहायता": "Religious and Cultural Institution Assistance",
    "भैपरी आउने चालु खर्च": "Current Expenses Due to Emergencies",
    "विविध कार्यक्रम खर्च": "Miscellaneous Program Expenses",
    "मृत कर्मचारीको सुविधा तथा सहायता": "Facilities and Assistance for Deceased Employees",
    "राजस्व फिर्ता": "Revenue Return",
      "क्षयरोग सामान्य उपचार": "Tuberculosis General Treatment",
  "महामारीको रूपमा देखा पर्ने रोगहरूको रोकथाम र नियन्त्रण": "Prevention and Control of Epidemic Diseases",
  "विविध कार्यक्रम": "Miscellaneous Programs",
  "विशिष्ट व्यक्ति तथा प्रतिनिधि मण्डलको भ्रमण खर्च": "Travel Expenses for Distinguished Persons and Delegations",
  "पुस्तक तथा सामग्री खर्च": "Book and Material Expenses",
  "औषधीखरिद खर्च": "Medicine Purchase Expenses",
  "औषधीउपचार खर्च": "Medicine Treatment Expenses",
  "अन्य सामाजिक सुरक्षा": "Other Social Security",
  "सञ्चार सम्बन्धी उपकरण खरिद तथा जडान कार्य": "Purchase and Installation of Communication Equipment",
  "सडक ट्रयाक निर्माण": "Road Track Construction",
  "स्तरोन्नती तथा मर्मत": "Upgrading and Repair",
  "कृषि सडक निर्माण तथा मर्मत": "Agricultural Road Construction and Repair",
  "कृषि सडक निर्माण र स्तरोन्नति तथा मर्मत कार्यक्रम": "Agricultural Road Construction, Upgrading and Repair Program",
  "युवा प्रोत्साहन कार्यक्रम": "Youth Encouragement Program",
  "सरसफाई र फोहर व्यवस्थापनकार्यक्रम": "Cleaning and Waste Management Program",
  "सर्ने, नसर्ने, जुनोटीक तथा मानसिक स्वास्थ्य सम्बन्धि विविध कार्यक्रम": "Various Programs on Mental Health and other related areas",
  "प्रसूती सेवा कार्यक्रम": "Maternity Service Program",
  "मृगौला रोग सेवा": "Kidney Disease Service",
  "नवजात शिशु तथा वालरोगको एकीकृत व्यवस्थापन": "Integrated Management of Newborn and Childhood Illnesses"

}


category_nepali_to_english = {
    "यातयात पूर्वाधार": "Transport Infrastructure",
    "भवन मर्मत सुधार": "Building Repair and Improvement",
    "विपद व्यवस्थापन": "Disaster Management",
    "पोषण कार्यक्रम": "Nutrition Program",
    "कार्यालय सञ्चालन तथा प्रशासनिक": "Office Operation and Administration",
    "प्रशासकीय सुशासन": "Administrative Good Governance",
    "भाषा तथा संस्कृति": "Language and Culture",
    "भवन, आवास तथा सहरी विकास": "Building, Housing, and Urban Development",
    "औषधि खरिद": "Medicine Procurement",
    "सामाजिक सुरक्षा तथा संरक्षण": "Social Security and Protection",
    "लैंगिक समानता तथा सामाजिक समावेशीकरण": "Gender Equality and Social Inclusion",
    "सहकारी": "Cooperative",
    "खानेपानी तथा सरसफाई": "Drinking Water and Sanitation",
    "वालरोग व्यवस्थापन": "Child Disease Management",
    "शिक्षा": "Education",
    "वाणिज्य": "Commerce",
    "उर्जा": "Energy",
    "आयुर्वेद सेवा": "Ayurveda Service",
    "वातावरण तथा जलवायु": "Environment and Climate",
    "जलश्रोत तथा सिंचाई": "Water Resources and Irrigation",
    "कालाजार नियन्त्रण": "Kala-Azar Control",
    "योजना तर्जुमा र कार्यन्वयन": "Plan Formulation and Implementation",
    "किशोरकिशोरी स्वास्थ्य": "Adolescent Health",
    "महिला स्वास्थ्य स्वयं सेविका": "Female Health Volunteer",
    "कृषि": "Agriculture",
    "युवा तथा खेलकुद": "Youth and Sports",
    "शान्ति तथा सुव्यवस्था": "Peace and Order",
    "श्रम तथा रोजगारी": "Labor and Employment",
    "वित्तीय सुशासन": "Financial Good Governance",
    "बिज्ञान तथा प्रबिधि": "Science and Technology",
    "परिवार नियोजन": "Family Planning",
    "पशुपन्छी विकास": "Livestock Development",
    "पर्यटन": "Tourism",
    "क्षमता अभिबृध्दि": "Capacity Development",
    "तथ्यांक प्रणाली": "Statistical System",
    "खोप कार्यक्रम": "Vaccination Program",
    "अनुगमन तथा मुल्यांकन": "Monitoring and Evaluation",
    "सुरक्षित मातृत्व": "Safe Motherhood",
    "स्वास्थ्य शिक्षा तथा सूचना": "Health Education and Information",
    "क्यान्सर सेवा": "Cancer Service",
    "मानब संशाधन विकास": "Human Resource Development",
    "सम्पदा पूर्वाधार": "Heritage Infrastructure",
    "पुननिर्माण": "Reconstruction",
    "संचार तथा सूचना प्रबिधि": "Communication and Information Technology",
    "गरिबी निवारण": "Poverty Alleviation",
    "अन्य सघ संस्था/केन्द्र": "Other Federal Institutions/Centers",
    "सरूवा रोग नियन्त्रण": "Communicable Disease Control",
    "महामारी रोग नियन्त्रण": "Epidemic Disease Control",
    "जुनोटिक रोग नियन्त्रण": "Zoonotic Disease Control",
    "गुणस्तर मापन तथा नियमन": "Quality Measurement and Regulation",
    "भवन निर्माण": "Building Construction",
    "फर्निचर तथा फिक्चर्स": "Furniture and Fixtures",
    "कृषि र पशुपालन": "Agriculture and Livestock",
    "सिचार्इ": "Irrigation",
    "बालिका र महिला सशक्तिकरण": "Empowerment of Girls and Women",
    "सामाजिक सुरक्षा": "Social Security",
    "शिक्षा": "Education",
    "स्वास्थ्य": "Health",
    "खानेपानी र सरसफाइ": "Water Supply and Sanitation",
    "धार्मिक र सांस्कृतिक": "Religious and Cultural",
    "बिपत व्यवस्थापन": "Disaster Management",
    "सडक र पुल": "Roads and Bridges",
    "अन्तराष्ट्रिय अनुदान": "International Grants",
    "सूचना र सञ्चार": "Information and Communication",
    "बैंकिङ्ग सेवा": "Banking Services",
    "विपद् प्रबन्धन": "Disaster Management",
    "कानुनी": "Legal",
    "प्रविधि": "Technology",
    "स्थानिय विकास": "Local Development",
    "पर्यटन": "Tourism",
    "उर्जा र जलविद्युत": "Energy and Hydropower",
    "वातावरण": "Environment",
    "उद्योग": "Industry",
    "सार्वजनिक यातायात": "Public Transportation",
    "युवा र खेलकुद": "Youth and Sports",
    "सम्पदा संरक्षण": "Heritage Conservation",
    "सामाजिक विकास": "Social Development",
    "नागरिक सुविधाहरु": "Citizen Amenities",
    "वाणिज्य र व्यवसाय": "Commerce and Business",
    "रोजगारी": "Employment",
    "प्रजातान्त्रिक प्रणाली": "Democratic Systems",
    "राजनितिक": "Political",
    "सामाजिक अभियान": "Social Campaign",
    "सांस्कृतिक कार्यक्रम": "Cultural Events",
    "जनस्वास्थ्य": "Public Health",
    "सुरक्षा तथा सम्बन्ध": "Security and Relations",
    "मानव अधिकार": "Human Rights",
    "विदेशी सहयोग": "Foreign Aid",
    "प्राकृतिक संरक्षण": "Natural Conservation",
    "अध्ययन र अनुसन्धान": "Study and Research",
    "विदेशी व्यापार र लगानी": "Foreign Trade and Investment",
    "विकास सहयोग": "Development Cooperation",
    "निगरानी र मूल्यांकन": "Monitoring and Evaluation",
    "सामाजिक संजाल": "Social Networking",
    "सूचना प्रविधि": "Information Technology",
    "गैर सरकारी सहयोग": "Non-Governmental Cooperation",
    "शासन तथा व्यवस्थापन": "Governance and Management",
    "स्वास्थ्य सुधार": "Health Improvement",
    "अनुसन्धान तथा विकास": "Research and Development",
    "स्थानिय उत्पादन": "Local Production",
    "गुणस्तर नियन्त्रण": "Quality Control",
    "श्रम तथा रोजगारी": "Labor and Employment",
    "सामुदायिक सुधार": "Community Improvement",
    "पर्यटकीय संवर्द्धन": "Tourism Promotion",
    "अन्तर्राष्ट्रीय मामिला": "International Affairs",
    "स्थानिय प्रशासन": "Local Administration",
    "नागरिक सम्बन्ध": "Citizen Relations",
    "विज्ञान तथा प्रविधि": "Science and Technology",
    "बाल विकास": "Child Development",
    "ग्रामीण विकास": "Rural Development",
    "अर्थतन्त्र": "Economy",
    "जनशक्ति विकास": "Manpower Development",
    "शान्ति र सुरक्षा": "Peace and Security",
    "अन्तरराष्ट्रिय सम्बन्ध": "International Relations",
    "राज्य निर्माण": "State Building",
    "पर्यावरण संरक्षण": "Environmental Protection",
    "बजेट र वित्तीय": "Budget and Financial",
    "जनसंख्या": "Population",
    "विश्वसनीयता सुधार": "Reliability Improvement",
    "मानव संसाधन": "Human Resources",
    "जनस्वास्थ्य र निर्माण": "Public Health and Construction",
    "शैक्षिक विकास": "Educational Development",
    "संस्कृति र साहित्य": "Culture and Literature",
    "अध्यात्मिक विकास": "Spiritual Development",
    "राष्ट्रिय सुरक्षा": "National Security",
    "राष्ट्रिय एकता": "National Unity",
    "समाज सेवा": "Social Service",
    "जनसेवा": "Public Service",
    "समाजिक समरसता": "Social Harmony",
    "जनसंपर्क": "Public Relations",
    "सम्पदा संरक्षण र सम्वर्द्धन": "Heritage Conservation and Enhancement",
    "पर्यावरणीय प्रबन्धन": "Environmental Management",
    "अन्तर्राष्ट्रीय सहयोग": "International Cooperation",
    "जनसांख्यिकी": "Demographics",
    "नवीकरणीय उर्जा": "Renewable Energy",
    "बहुआयामिक विकास": "Multidimensional Development",
    "वित्तीय संरक्षण": "Financial Protection",
    "व्यापार विकास": "Trade Development",
    "अनुगमन र नियमन": "Monitoring and Regulation",
    "विश्व व्यापी सहयोग": "Global Cooperation",
    "अधिकार र कर्तव्य": "Rights and Duties",
    "सामाजिक न्याय": "Social Justice",
    "नागरिक संरक्षण": "Citizen Protection",
    "राज्य संचालन": "State Governance",
    "नागरिकता": "Citizenship",
    "सामाजिक सुरक्षा योजना": "Social Security Plan",
    "नागरिक अधिकार": "Citizen Rights",
    "सरकारी संरचना": "Government Structure",
    "सामाजिक संगठन": "Social Organization",
    "नागरिक सम्पन्नता": "Citizen Prosperity",
    "उद्यमशीलता विकास": "Entrepreneurship Development",
    "स्वास्थ्य सेवा": "Health Services",
    "बाल सुरक्षा": "Child Protection",
    "अधिकार र प्रभाव": "Rights and Impact",
    "सामुदायिक विकास": "Community Development",
    "रोजगार सुविधा": "Employment Facility",
    "सामुदायिक स्वास्थ्य": "Community Health",
    "आर्थिक विकास": "Economic Development",
    "व्यापार सुधार": "Business Improvement",
    "संगठनात्मक विकास": "Organizational Development",
    "विकास नीति": "Development Policy",
    "स्वास्थ्य विकास": "Health Development",
    "जनाधिकार": "People's Rights",
    "नीति निर्धारण": "Policy Making",
    "राष्ट्रिय विकास": "National Development",
    "सामाजिक सेवा": "Social Service",
    "शैक्षिक सुधार": "Educational Reform",
    "निर्माण र विकास": "Construction and Development",
    "रोजगार नीति": "Employment Policy",
    "सामाजिक उत्थान": "Social Upliftment",
    "न्यायिक सुधार": "Judicial Reform",
    "रोजगार अवसर": "Employment Opportunity",
    "उच्च शिक्षा": "Higher Education",
    "सामाजिक नवीकरण": "Social Renewal",
    "आर्थिक विकास": "Economic Development",
    "भूमि व्यवस्था": "Land Management",
  "क्षयरोग नियन्त्रण": "Tuberculosis Control",
  "किटजन्य रोग": "Vector-Borne Disease",
  "जनशक्ति अभिलेख": "Manpower Records",
  "मेसिनरी औजार": "Machinery Tools"

}

column_translation_map = {
    "क्र.सं.": "Serial No.",
    " मुख्य कार्यक्रम/मुख्य क्रियाकलाप": "Main Program/Main Activity",
    "विनियोजन": "Allocation",
    "खर्च": "Expenditure",
    "खर्च (%)": "Expenditure (%)",
    "मौज्दात": "Remaining Balance",
    "वर्ग": "Category"
}


@router.get('/local_activities')
def get_compare_data(वर्ग :List[str]= Query(...), कार्यक्रम: List[str]= Query(None), cities: List[str] = Query(None), years:List[str] = Query(None), nepali:bool=Query(True)):

    var={}

    combined_data=[]

    if not cities:
        cities=['lekbeshi','birgunj','janakpur','tulsipur','shuddhodhan']

    if not years:
        years=['2080-81','2081-82','2082-83','2083-84']

    for city in cities:
        for year in years:
            # Construct the path to the JSON file
            directory_path=f"/app/backend/data/{city}/local_activities/{year}"
            file_path = f"/app/backend/data/{city}/local_activities/{year}/data.json"
            
            # Ensure the directory exists
            if not os.path.exists(directory_path):
                # print('error')
                continue

            # # Check if the file exists
            if not os.path.exists(file_path):
                save_json(directory_path=directory_path,city=city,year=year)

            with open(f'/app/backend/data/{city}/local_activities/{year}/data.json','r') as f:
                json_data=json.load(f)

            # print(json_data)
            # Create a DataFrame for the city and year
            df_key = f"{city}_{year.replace('-','_')}"
            var[df_key] = pd.DataFrame(json_data['data'])

            # Register the DataFrame in DuckDB
            duckdb.register(df_key, var[df_key])

            combined_data.append(\
                    duckdb.query(f'''
                    select
                        *,
                        '{city}' as city,
                        '{year}' as year
                    from {df_key}
                ''').to_df()\
            )

    # Concatenate all query results into a single DataFrame
    final_df = pd.concat(combined_data, ignore_index=True)
    final_df[' मुख्य कार्यक्रम/मुख्य क्रियाकलाप'] = final_df[' मुख्य कार्यक्रम/मुख्य क्रियाकलाप'].fillna('')
    # Clean up spaces in 'Main Program/Main Activity' or any other field
    final_df[' मुख्य कार्यक्रम/मुख्य क्रियाकलाप'] = final_df[' मुख्य कार्यक्रम/मुख्य क्रियाकलाप'].apply(lambda x: re.sub(r'\s+', ' ', x).strip())

    if not nepali:
        final_df = final_df.rename(columns=column_translation_map)
        final_df['Main Program/Main Activity'] = final_df['Main Program/Main Activity'].map(title_nepali_to_english).fillna(final_df['Main Program/Main Activity'])
        final_df['Category'] = final_df['Category'].map(category_nepali_to_english).fillna(final_df['Category'])
        
    # final_df['बजेट उपशीर्षक नाम'] = final_df['बजेट उपशीर्षक नाम'].fillna('')
    # final_df['खर्च पूंजीगत'] = final_df['खर्च पूंजीगत'].fillna(0)  # If numeric, fill with 0
    if वर्ग[0]=='total':
        data= final_df[final_df['क्र.सं.']=='कुल जम्मा'].to_dict(orient="records")
    elif वर्ग[0]=='all':
        # final_df.to_csv('./local_activites.csv',index=False)
        data= final_df.to_dict(orient="records")
        # return final_df[final_df['क्र.सं.']!='जम्मा'].to_dict(orient="records")
    else:
        # उपशीर्षक.append('')
        data= final_df[final_df['वर्ग'].isin(वर्ग)].to_dict(orient="records")

    return {
        "filterOptions":{
            "years": os.listdir(f"/app/backend/data/{cities[0]}/local_activities")
        },
        "data":data
    }

# @router.get("/local_activities")
# def get_all_local_activities_data():
#     """
#     Endpoint to get quadrimester expense data for all cities and all years.
#     """
#     directory_root = "./data/"  # Root directory where cities are stored
#     all_cities_data = []
#     try:
#         # Iterate over all city directories in the root directory
#         for city_folder in os.listdir(directory_root):
#             city_path = os.path.join(directory_root, city_folder, "local_activities")
            
#             if os.path.isdir(city_path):  # Ensure it's a directory
#                 city_data = {"idms": city_folder, "years": []}
                
#                 # Iterate over all year directories in the city's local_activities folder
#                 for year_folder in os.listdir(city_path):
#                     year_path = os.path.join(city_path, year_folder)
#                     json_path = os.path.join(year_path, "data.json")
                    
#                     if os.path.exists(json_path):
#                         # Load JSON data for each year
#                         with open(json_path, "r", encoding="utf-8") as json_file:
#                             year_data = json.load(json_file)
#                         city_data["years"].append({
#                             "year": year_folder,
#                             "data": year_data
#                         })
#                     else:
#                         # If no JSON exists, attempt to process the Excel file
#                         xlsx_files = glob.glob(os.path.join(year_path, "*.xlsx"))
#                         if xlsx_files:
#                             excel_path = xlsx_files[0]
#                             json_data = local_activities_to_json(city=city_folder, year=year_folder, excel_path=excel_path)
#                             if json_data:
#                                 city_data["years"].append({
#                                     "year": year_folder,
#                                     "data": json_data
#                                 })
#                             else:
#                                 print(f"Failed to process Excel data for {year_folder} in {city_folder}")
#                         else:
#                             print(f"No data found for {year_folder} in {city_folder}")
                
#                 all_cities_data.append(city_data)
        
#         return {
#             "status": 200,
#             "topic": "local_activities",
#             "data": all_cities_data
#         }

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")

# @router.get("/local_activities/{city}")
# def get_local_activities_data(city: str, year: Optional[str] = None):
#     directory_root = f"./data/{city}/local_activities/"
#     if year:
#         directory_path = f"{directory_root}{year}"
#         json_path = f"{directory_path}/data.json"

#         if os.path.exists(json_path):
#             print("JSON File Already Exists! No need for further processing.")
#             with open(json_path, "r", encoding="utf-8") as json_file:
#                 data = json.load(json_file)
#             return {"status": 200, "idms": city, "year": year, "topic": "local_activities", "data": data}
#         else:
#             print("No JSON file found, processing the Excel file.")
#             xlsx_files = glob.glob(os.path.join(directory_path, "*.xlsx"))
#             if xlsx_files:
#                 excel_path = xlsx_files[0]
#                 try:
#                     json_data = local_activities_to_json(city=city, year=year, excel_path=excel_path)
#                     return {"status": 200, "idms": city, "year": year, "topic": "local_activities", "data": json_data}
#                 except Exception as e:
#                     raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
#             else:
#                 raise HTTPException(status_code=404, detail="Excel file not found")
#     else:
#         # Process data for all years
#         all_data = []
#         if not os.path.exists(directory_root):
#             raise HTTPException(status_code=404, detail="City directory not found")
#         try:
#             year_folders = [folder for folder in os.listdir(directory_root) if os.path.isdir(os.path.join(directory_root, folder))]
#             for year_folder in year_folders:
#                 json_path = os.path.join(directory_root, year_folder, "data.json")
#                 if os.path.exists(json_path):
#                     with open(json_path, "r", encoding="utf-8") as json_file:
#                         year_data = json.load(json_file)
#                     all_data.append({"year": year_folder, "data": year_data})
#                 else:
#                     print(f"No JSON data found for {year_folder}, attempting to process Excel file.")
#                     xlsx_files = glob.glob(os.path.join(directory_root, year_folder, "*.xlsx"))
#                     if xlsx_files:
#                         excel_path = xlsx_files[0]
#                         try:
#                             json_data = local_activities_to_json(city=city, year=year_folder, excel_path=excel_path)
#                             all_data.append({"year": year_folder, "data": json_data})
#                         except Exception as e:
#                             print(f"Failed to process Excel data for {year_folder}: {str(e)}")
#                     else:
#                         print(f"No Excel data found for {year_folder}")
#             return {"status": 200, "idms": city, "topic": "local_activities", "data": all_data}
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")


# @router.get("/local_activities/{city}")
# def get_local_activities_data(city: str, year: Optional[str] = None):
#     directory_root = f"./data/{city}/local_activities/"
#     if year:
#         directory_path = f"{directory_root}{year}"
#         json_path = f"{directory_path}/data.json"

#         if os.path.exists(json_path):
#             print("JSON File Already Exists! No need for further processing.")
#             with open(json_path, "r", encoding="utf-8") as json_file:
#                 data = json.load(json_file)
#             return {"status": 200, "idms": city, "year": year, "topic": "local_activities", "data": data}
#         else:
#             print("No JSON file found, processing the Excel file.")
#             xlsx_files = glob.glob(os.path.join(directory_path, "*.xlsx"))
#             if not xlsx_files:
#                 raise HTTPException(status_code=404, detail="Excel file not found")
#             excel_path = xlsx_files[0]
#             try:
#                 json_data = local_activities_to_json(city=city, year=year, excel_path=excel_path)
#                 return {"status": 200, "idms": city, "year": year, "topic": "local_activities", "data": json.loads(json_data)}
#             except Exception as e:
#                 raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
#     else:
#         # Process data for all years
#         all_data = []
#         if not os.path.exists(directory_root):
#             raise HTTPException(status_code=404, detail="City directory not found")
#         try:
#             year_folders = [folder for folder in os.listdir(directory_root) if os.path.isdir(os.path.join(directory_root, folder))]
#             for year_folder in year_folders:
#                 json_path = os.path.join(directory_root, year_folder, "data.json")
#                 if os.path.exists(json_path):
#                     with open(json_path, "r", encoding="utf-8") as json_file:
#                         year_data = json.load(json_file)
#                     all_data.append({"year": year_folder, "data": year_data})
#                 else:
#                     print(f"No data found for {year_folder}")
#             return {"status": 200, "idms": city, "topic": "local_activities", "data": all_data}
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")


# @router.get("/local_activities/{city}")
# def get_local_activities_data(city:str, year: Optional[str] = None):
#     directory_root="./data/"+city+"/local_activities/"
#     if year is not None:
#         directory_path=directory_root+year
#         json_path = directory_path+"/data.json"

#         if os.path.exists(json_path):
#             print("JSON File Already Exists! No need further processing")
#             # Open the JSON file and load its content
#             with open(json_path, "r", encoding="utf-8") as json_file:
#                 data = json.load(json_file)
            
#             return {
#                 "status":200,
#                 "idms":city,
#                 "year":year,
#                 "topic":"local_activities",
#                 "data": data
#             }
        
#         else:
#             print("No json file found, have to process entire excel file")

#             # Check if the file exists
#             # Find all .xlsx files in the given directory
#             xlsx_files = glob.glob(os.path.join(directory_root+year, "*.xlsx"))
#             # print(xlsx_files)
#             if xlsx_files:
#                 excel_path=xlsx_files[0]
#             else:
#                 raise HTTPException(status_code=404, detail="Excel file not found")
            
#             try:
#                 json_data=local_activities_to_json(city=city, year=year, excel_path=excel_path)

#                 return {
#                     "status": 200,
#                     "year":year,
#                     "idms":city,
#                     "topic":"local_activities",
#                     "data": json_data
#                 }

#             except Exception as e:
#                 raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
        
#     else:
#         # Return data for all year
#         all_data = []
#         try:
#             # List all subdirectories in the root directory (representing years)
#             for year_folder in os.listdir(directory_root):
#                 # print(year_folder)
#                 directory_path = directory_root+year_folder
#                 # print(directory_path)
#                 json_path = os.path.join(directory_path, "data.json")
                
#                 if os.path.exists(json_path):
#                     # Load JSON data for each year
#                     with open(json_path, "r", encoding="utf-8") as json_file:
#                         year_data = json.load(json_file)
#                     all_data.append({
#                         "year": year_folder,
#                         "data": year_data
#                     })
#                 else:
#                     # If no JSON exists, attempt to process the Excel file
#                     # excel_path = os.path.join(directory_path, "data.xlsx")
#                     file_path = directory_root+year

#                     # Find all .xlsx files in the given directory
#                     xlsx_files = glob.glob(os.path.join(file_path, "*.xlsx"))
#                     excel_path=xlsx_files[0]

#                     if os.path.exists(excel_path):
#                         json_data = local_activities_to_json(city=city, year=year, excel_path=excel_path)
#                         all_data.append({
#                             "year": year_folder,
#                             "data": json.loads(json_data)
#                         })
#                     else:
#                         print(f"No data found for {year_folder}")
            
#             return {
#                 "status": 200,
#                 "idms":city,
#                 "topic":"local_activities",
#                 "data": all_data
#             }
        
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")



