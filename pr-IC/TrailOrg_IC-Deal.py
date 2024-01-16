#!/usr/bin/env python
# coding: utf-8

# In[ ]:


################## On Priority for deal ID's  ##############################################################

## libraries
import spacy
import pandas as pd
import nltk
from sklearn import svm
from spacy.matcher import Matcher
from nltk.tokenize import PunktSentenceTokenizer
from sklearn.multiclass import OneVsRestClassifier
from sklearn.feature_extraction.text import CountVectorizer
from transformers import AutoModelForQuestionAnswering, AutoTokenizer, pipeline
nlp_ic = spacy.load("/home/azureuser/ic_model1")
sector = pd.read_excel("/home/azureuser/subsec-to-use.xlsx")
import os
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
import pandas as pd
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

### input data fetch from cosmos
import uuid
from azure.cosmos import exceptions, CosmosClient, PartitionKey
import json
from datetime import date
import pandas as pd
import numpy as np
import time
import pathlib
from tqdm import tqdm
from datetime import date

endpoint='https://cdb-universe.documents.azure.com:443/'
key='cB0SZcfcoApRqaMUDZdLlc8Do1CvOvPOcXUeefpRFHwcMOsneFLqR6lvAQXnBynZ1a6PhURVZtboACDbXUg6Dw=='
client=CosmosClient(endpoint,credential=key)
heimdall_db= 'heimdall-db'
deal_id = 'Deal-Id'

database_1 = client.get_database_client(heimdall_db)
container_1 = database_1.get_container_client(deal_id)
query = "SELECT c.id, c.Corrected_Investee, c.Art_Id, c.Sector, c.Status FROM c WHERE c.Status != 'Invalid Article'"
items = container_1.query_items(query, enable_cross_partition_query=True)
item_list = list(items)
deal_id = pd.DataFrame(item_list)
print(deal_id.shape)

new_df=deal_id[(deal_id['Sector'].isnull())]
df = new_df.explode('Art_Id')
data = df["Art_Id"].to_list()

container_name = "art-id"
database_name = "cdb-L1"
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

new_df = pd.DataFrame()

for art_id in data:
    query = f"SELECT c.id, c.content_cl, c.published_date FROM c WHERE c.id = '{art_id}'"
    items = container.query_items(query, enable_cross_partition_query=True)
    item_list = list(items)
    row = pd.DataFrame(item_list)
    new_df = pd.concat([new_df, row], ignore_index=True)

# Banking and Finance
Class1 = ["Personal finance", "Lending", "Account Receivable Solution", "App-Based Wallet", "Asset Management", "Banking And Finance", "Bookkeeping And Accounting Solutions", "Business Expense Management", "Capital Finance", "Cashback And Coupons App", "Cashflow Management", "Community Monetization ", "Credit Financing", "Credit Scoring", "Debt Financing", "Digital Banking", "Digital Wallet", "Diversified Investment Management", "Education Loans", "Expense Management", "Financial Services", "Fintech", "Neo-Bank", "Payment App", "Payment Solutions", "Point-Of-Sale Financing", "Prepaid Card", "Purchase Financing", "QR Based Payment", "Secure Payment", "Stocks And Mutual Fund Investment", "Term Loans Provider", "UPI Payments", "Revenue-Based Financing marketplace", "advanced QR technology", "private equity and merchant banking", "giving day and crowdfunding technology."]

# Education and Training
Class2 = ["Overseas education", "Classroom Technology", "Curriculum Development", "Education Software", "Educational Experiences", "Educational Leadership", "Educational Outreach", "Educational Trends", "Student Support Services", "Bootcamp", "Classroom Management", "Digital Learning Provider", "Doubt Solving", "Edtech", "Education Infrastructure Solutions", "E-Learning", "Exam Preperation", "Exams Learning", "Interview Preparation ", "transformative learning experiences", "Online Learning", "Online Teaching", "Student Evaluation", "people development solution", "digital coach provider"]

# HealthCare & Pharmacuticals
Class3 = ["FemTech","health services", "Disease Management", "Diagnostic Test Services", "Health", "Patient care", "Clinical trials", "Medical research", "Electronic health records", "pharmacy ", "Retail Pharmacy", "Telemedicine", "Medicine", "Medical", "biopharmaceutical", "drug discovery platform", "drug discovery", "drug development", "independent primary care practices"]

# BioTechnology
Class4 = ["Biotech", "immunotherapy", "human therapeutics", "RNA-based therapeutics", "biomedical genetics", "prenatal test", "avian genetic", "gene-based medicines", "neurodegenerative", "generic biocatalytic solutions", "recombinant proteins", "BioPharma", "nutraceutical products", "biopharmaceutical company", "biosimilars Developer", "molecular biology research", "genetic variation and function", "DNA sequencing giant", "clinical-stage biotech company"]

# HealthTech
Class5 = [" Cloud-based hospital management system", "Digital Fitness", "Employee Health Management", "Hospital Management Software", "Care Management", "Digital Healthcare", "Health Chatbot", "Healthtech", "Online Doctor Consultation", "Picture Archive And Communication", "Radiology", "Radiology Workflow Solution", "Tele-Consultation", "Virtual Consultations", "telemedicine platforms", "oncology-focused software solutions", "smart preventive healthcare company"]

# Food Tech
Class6 = ["restaurant management platform", "food ordering and vendor management", "Cloud kitchen ", "Food Technology", "Coffee Service Operators", "Coffee Shops Operators", "Automated Order-Taking App", "Limited-Service Restaurants Serving Tea", "Cafeteria Management Solution", "F&B Operations", "Foodtech", "Food-Tech", "online grocery shopping", "online supermarket", "food and grocery retail", "limited service restaurant offering burgers", "burger chain ", "food delivery platform"]

# Automotive
Class7 = ["Auto Repair And Maintenance Services", "Automotive Digital", "Autotech", "Car Cleaning", "automotive repairing services", "Car Search", "Car Servicing", "Edge Ai Automotive", "Electric Vehicles", "Fleet Operations Management", "auto parts distribution", "cars and commercial automotive vehicles", "automobile manufacturer", "commercial vehicle manufacturer", "premium cars manufacturer", " car listing portal", " online garage aggregator", "automotive aftermarket ", "doorstep automotive", "Automobile marketplace"]

# Life Science
Class8 = ["Genetic Testing Services", "Drug Discovery", "Genome", "Genomic Solutions", "Genomics-Driven", "Diagnostics", " cancer-test developer ", "serving science", "single-molecule analysis", "fueling scientific discovery", "biomarker", "clinical-stage CRISPR ", "genome-editing ", "clinical-stage speciality pharmaceutical", "clinical trial management software", "gene editing-based cultured ingredients", "microbiome therapeutics"]

# Environment tech
Class9 = ["renewable generation market", "Refurbished Gadgets", "Clean Technology", "Energy efficient solutions", "Environement Tech", "Green Technology", "Waste Management Technology", " Energy generation", "supporting environmental causes", "green concrete technologies", "carbon capture innovator", "clean energy and industrial gas markets", "climate technology company", "decarbonization", "geothermal waste heat recovery products", "utility bill management", "renewable energy certificates", "Climate-tech software company", "global climate company", "biodegradable biopolymer", "sustainable tech company", "renewables developer", "sustainable mobile filtration solutions"]

# Business Services
Class10 = ["cloud communication", "business services", "CRM", "customer relationship management", "digital marketing solutions", "Consulting", "Event Planning", "Training services", "Hrms Platform", "Crm Platform", "Digital Onboarding", "business process outsourcing services", "digital transformation services", "management strategy", " business brokerage services", " human resource management software", "cyber defense company ", "Meetings Marketplace", " sales and marketing platform", "creative-tech marketing agency"]

# TechWear
Class11 = ["audio devices", "Audio Industry", "Earwear", "smartband", "smartwear", "Techwear", "wearable technology", "smartwatches", "smart clothing with integrated sensors ", "smart fitness trackers ", "smart shoes ", "wearable medical devices", "wearable wristband integrated", "fitness tracking devices", "Online retailer of connected wearable devices", "personal smart devices", "AI-powered Bluetooth headset"]

# personal care products and fashion tech
Class12 = ["Beauty Ecommerce", "Cosmetic market", "FashionTech", "Grooming brands", "Hygiene Product", "Organic Beauty", "Organic SkinCare", "Personal Care Brand", "luxury fashion retail platform ", "fashion rental service", "AI-powered shopping platform ", "cosmetics and beauty company", "Dermatological Beauty", "Online platform offering customized apparels", "global brand incubator", "fast fashion e-tailer", "AI powered shopping destination", "Footwear and accessories brand", " D2C beauty subscription service"]

# Internet and E-commerce
Class13 = ["Community Commerce Platform", "Multi-Category E-Commerce Platform", "Catalog Management Solutions", "Ecommerce Tech", "Electronic Products", "Mattress Ecommerce", "Merchant Commerce", "Shopping Marketplace", "commerce solutions for fashion businesses", "marketplace platform", "E-commerce and classifieds giant", "online retailer", "global marketplace for unique and creative goods", "furniture and home goods marketplace", "e-retailer of furniture and home furnishings ", "Online furniture marketplace", "multi-branded e-commerce platform ", " Interior Design Solutions", "Home Furnishing Ecommerce", "Multi-Category Home Product", "Home Tech", "Interior Design Platform", "Interior Design Tech", "Virtual Space", "smart home solutions", "smart home manufacturers ", "Integrated social and digital agency", "interactive and shoppable streaming platform"]

# Media and Entertainment
Class14 = ["Voice Calling And Internet Services", "Event Ticketing", "Microblogging App", "Online Publisher", "Community Commerce", "Digital tool", "Social Media", "Video Sharing", "video streaming platform", "Virtual Event Management Software", "Virtual Events", "OTT video on-demand service", "video content streaming platform", " Video Experience Cloud", "TV Platform", "Hyperlocal social media platform", "short-form video platform"]

# Construction and Infrastructure
Class15 = ["PropTech","Residential Properties Platform", "Real-Estate Platform", "Construction Technology", "Real Estate Service Provider", "Real Estate Tech", "construction and concessions company", "construction and infrastructure company", "real estate infrastructure development services", "app-based listing platform for residential properties", "real estate brokerage platform", "hourly venue rentals", "Co-working and event space provider", "coworking startup", "real estate developer"]

# retail
Class16 = ["Retail Chain", "Retail Operating System", "Retail Stores", "Consumer Products", "FMCG", "online retailer", "Retailer of home furnishing products", "Value fashion retailer", "retail distribution company", "Retail tech platform ", "Smartphone retail chain", "ERP software solutions for retail businesses", "supply chain and retail optimization company", "fashion retail and consumer goods product", "global footwear company", " omnichannel jeweler"]

# Insurance
Class17 = ["Insurance Platform", "Insurtech", "Insurance Technology", "Insurance", "Health Insurance", "digital insurance compan", "high-profile insurtech", "specialty reinsurance and insurance solutions", "general insurance business", "Online platform for health and P&C insurances", "no-code insurance platform", "insurance premium payment platform", "AI-driven cyber insurance platform", "life insurance services"]

# Data and Analytics
Class18 = ["AI Based", "AR shopping experiences", "Computer Vision", "Natural Langauge Processing", "Data analytics", "Machine Learning", " financial market data solutions", "platform leveraging machine learning", "AI based web conferencing software provider", "Ai-Powered Customer Intelligence Platform", "AI-driven cyber platform", " platform for Everyday AI", " multicloud data giant", "data-enabled solutions company", " data science and AI solutions", " enterprise customer data platform", " SaaS-based speech recognition device and solutions", "AI managed solution for active wildfire detection"]

# Aviation and Aerospace
Class19 = [" Space transportation", "launch vehicles", "Aerospace", "Starship flight test", " electric vertical take-off and landing (eVTOL) aircraft", "landing air taxi", "all-electric aircraft", "autonomous drone systems", "Drone manufacturer", "autonomous flight", "all-electric aircraft for commercial passenger service", "autonomous cargo aircraft systems", "autonomous VTOL aerial cargo system", "aerospace and technology", "aerospace manufacturer Reaction Engines", "aerospace engineering firm ", "propulsion solutions for small satellites ", "Rocket Engine Startup"]

# Sports and Recreation
Class20 = ["Gaming", "Online Gaming", "Daily Fantasy Sports ", "Innovative Toy And Game Company", "Online Esports Gaming Organization", "E-Sports", "Gaming platform ", "Sports-betting company", "digital sports entertainment and gaming company", "fantasy sports business", "premier mobile gaming destination", "tier-one online sportsbook", "Fantasy sports startup", "online gaming platform", "online skill gaming platform", "Mobile game maker", "developer of free-to-play mobile and social games", "long-standing publisher of casual games", "mobile gaming platform and developer"]

# Online dating and matchmaking Tech
Class21 = ["Dating Platform", "Social Connection Network", "Online Dating", "Matchmaking Tech", "women-centric dating and match making platform", "Dating app", "separate BFF app", "proximity-based dating and matchmaking platform", "dating application ", "freemium dating app", "live speed dating platform", "matchmaking feature", "Online dating powerhouse"]

# Hospitality and Tourism
Class22 = ["Travel Services", "Budget Hotels Brand", "Coffee Service Operators", "Coffee Shops Operators", "Automated Order-Taking App", "Limited-Service Restaurants Serving Tea", "Cafeteria Management Solution", "F&B Operations", "Budget Hotels Brand", "Travel And Hospitality", "global hospitality company", "travel booking platform ", "Online Travel Aggregator", "travel app", "tourism platform", " online vacation marketplace ", "Key Travel a travel management company"]

# ride hailing services
Class23 = ["bike taxis", "e-bikes", "Bike Taxi Service", "Car Rental", "Self-Driving Car Rental", "Electric Bicycle Rental Platforms", "Ride Hailing", "e-scooter rental", "electric mobility sharing ", "rival mobility aggregator", "ride-hailing and delivery markets", "ride-hailing startup", "GoCar services"]

# Textiles and Apparel
Class24 = ["Innerwear Ecommerce", "Innerwear Brand", "Fast Fashion", "Retail Fashion", "Clothing brand", "apparel", "tailoring", "stitching", "clothing manufacturer", "clothing line", " online fashion retailer", "products from independent designers", "luxury-quality leather", "fashion applications", "Circulose Supplier Network", "textile-to-textile recycling facility", "next-generation raw material", "yarn and textile producers"]

# Nonprofit and Philanthropy
Class25 = ["enterprise contact center", "curating tailored volunteering programs", "virtual volunteering experiences", " Nonprofit", "nonprofit educational institutions", "nonprofit organization", "non-sectarian nonprofit organization"]

# Journalism and Publishing
Class26 = ["App-based platform offering multi-category news", "news aggregator", "data-driven content house", "self-publishing startup", "ebook self-publishing", "self-publishing platform", "tools and distribution platform for visual stories ", "self-publishing service for authors", "book publishing platform", "social media-focused news division", "publisher technology platform"]

# Consumer Electronics
Class27 = ["consumer electronics", "Electronic Products", "Re-commerce marketplace", "refurbished mobile retail chain", "Refurbished electronics marketplace", "sells reconditioned electronics", "omnichannel personalization platform", "mobile phone processing solutions", "electronics recommerce firm", "High Tech and Media industries", "Finnish company that buys and sells refurbished smartphones", "refurbished iPhone provider", "electric bicycle company", "glass cover maker ", "global smartphone brand"]

# Electronic Manufacturing
Class28 = ["leading supplier of vacuum and abatement services", "photolithography technology developer", "electrical goods maker", "electronics giant", "Electronic products manufacturer", "semiconductor design", "multi category electronic product manufacturer", "leading electronics company", "high-performance semiconductor provider", "processors for mobile devices", "Microinverter supplier ", "wholesaler of solar and battery system",]

# Telecommunication
Class29 = ["Telecom", "Telecommunication", "4G Network", "Fibre Network", "unified communication solutions", "Information and communication Technology", "satellite communication", " telecom industry", "leading communications solutions provider", "digital communications company", "mobile network operators", "electronic communication services", "Cable TV & Fiber internet services", "affordable and reliable mobile broadband services", " industry-leading network.", "communication platform service provider", "high-speed fiber-optic broadband provider", "fiber-focused wired broadband ISP"]

# Agriculture and Farming
Class30 = ["Agrifood", " LiveStock Production", " agroforestry platform", "agricultural products", "Agro-Industry ", "poultry producer", "timberland", "wood producers", "feed manufacturer", " fishery", "shrimp processing", "dairy products", "meat and poultry", "Agriculture Tech", " Agritech", "smart farming", " agriculture supply chain", "agriculture solutions using IoT", "Agritech", "Farmtech", "hybrid crop and vegetable seeds"]

# oil refining Sector
Class31 = ["gold company", "mining industry", "Mining activities", "petroleum products", "Oil & Gas Downstream Services", "Mining support services", "crude oil storage terminal", "Storage solutions for oil industry", "junior oil and gas company", "oil and gas producer", "gas processing unit", "oilseeds processor", "oil pipeline", " pipeline operator", "Provider of oil refinery services", "oil & natural gas exploration services", "GOI Energy refinery complex"]

# Energy Sector
Class32 = ["power producer", "electric power generation", " air-conditioning and refrigeration", "HVAC industry", "energy management solutions", "independent and integrated energy company", "AI-enabled concentrating solar energy technology", " energy company", "Global solar energy business", "Energy utility", "power company", "home solar battery storage and solar energy-as-a-service provider", "residential solar battery storage and energy services company", "solar installer", "residential solar system providers", "Renewable Energy provider", "Renewable energy company ", "integrated energy companies"]

# Defense and Military
Class33 = [" Public administration and defence", "defense management", "multinational arms and security", "social security activities", " indigenous defence technology company", "Autonomous Drone Security System", "air defence solution", "aircraft integrated defeat system", "mission critical electronics for aerospace and defense", "missiles and defense business", "armament makers"]

# Management and Consulting
Class34 = ["Professional Networking platform", "professional membership organization", "Human Resource Management", "professional development resources", "document management solution", "HR outsourcing", "Tech advertisement", "Ai Marketing", "Content Marketing Software", "AI Advertising", "Internet of Things (IoT) platforms", "Lending-as-a-service platform", "Event management firm", "management and business consulting firm"]

# Gig Economy
Class35 = ["facilities support", "janitorial service", "domestic services", "housekeeping", "Cleaning Services", "manpower services", "Staffing Solutions", "carpooling service", "cleaning services company", "Provider of recruitment services"]

# Waste Management and Recycling
Class36 = ["waste and recycling industry", "recycling SaaS platform", "waste management and recycling SaaS platform", "green tech scale-up", "smart waste management solutions", "waste and recycling services", "waste and recycling metering technology", "waste metering company"]

# Manufacturing
Class37 = ["Hygiene goods", "Consumer Goods", " Lifestyle Manufacturer", "toilet paper manufacturer", "Abrasive manufacturer", "paper-based packaging manufacturing", "environmental services industry"]

# Veterinary activities
Class38 = ["animal feed", "pet care", "veterinary services", "veterinary medicine", "pet insurance", "animal health", "Veterinary supply", "services platform for veterinary clinics"]

# Food & Beverage
Class39 = ["Multinational food", "beverage corporation", "beverages products", "carbonated beverages", "confectionery food", "dairy products", "convenience foods"]

# Chemical Sector
Class40 = ["Chemical synthesis", "Chemical formulations", "Environmental compliance", "plastics", "polyolefins", "chemical products"]

# Import & Export Sector
Class41 = ["Export promotion", "Import sourcing", "Trade agreements", "Import duties and tariffs", "Freight forwarding", "International shipping", "Cross-border trade", "Trade compliance"]

# Industrial Manufacturing
Class42 = ["Product lifecycle", "Industrial machinery", "Supply chain management", "Assembly", "Inventory", "manufacturing", "Industrial Manufacturing"]

# Information Technology
Class43 = ["consulting", "technology services", "IT services", "digital transformation", "Software development", "Network infrastructure", "Information Technology", "project management", "Internet of Things", "Digitally native technology services company", "mobile Internet technology ", "digital transformation company", "contributor acknowledgment that gives software projects", "web conferencing software"]

# Saas Sector
Class44 = ["web-based DevOps", "cloud communications", "collaboration and project management", "payment processing", "customer messaging", "workflow automation", "graphic design platform", "video conferencing", "unified communication solutions", "Software as a service provider"]

# SeaFood Sector
Class45 = ["seafood", "salmon farming", "aquaculture", "wild-caught seafood", "salmon", "Coral reefs restoration", "fish farming", "fish processing"]

# Robotics and Automation
Class46 = [" robot maker","robo advisor platform", "Industrial robotics", "Automation", "automated robotic fulfillment and inventory optimization", "automated robotic fulfillment", "autonomous robotic picking solutions", "robotic picking ", "effective autonomous solutions ", "autonomous forklifts", "mobile robotics", "fellow AI robotic intelligence firm"]

# Water and Sanitation
Class47 = ["waste treatment ", "water and wastewater treatmen", "water technology company"]

# Legal and Law Enforcement
Class48 = ["shareholder rights law firm", "securities firm ", "Legal practive management solutions provider", "cloud-based legal technology", "legal operations software provider", "software for legal departments", "Legal cloud software"]

# Medical Devices
Class49 = ["medical equipment manufacturing", "medical diagnostic devices", "dialysis device ", "Global science and technology innovator", "diagnostic and surgical products for women", "Medical devices company"]

# Mining and Metals
Class50 = ["Suite Solutions for the mining industry", "multinational steel company", "steelmaker", "Stainless steel producer"]

# DeepTech
Class51 = ["liquidity and market-making for SNX tokens", "quantum photonics company ", "blockchain platform", "technology infrastructure company serving cryptocurrencies blockchains and enterprise-level projects", "utility-scale quantum compute", "Quantum computing developer", "quantum computing industry", "quantum computers using lasers", "cryptocurrency exchanges", "Cryptocurrency trading", "prop-tech space", "VR real estate spaces", "Augmentation Reality"]

# Transportation and Logistics Tech
Class52 = ["connectivity platform","Parking Management", "Smart Dispatch", "Fleet Management", "Freight Transportation", "Heavy Cargo Trucking Services", "Logistics And Distribution Industry", "Logistics Platform", "Logistics Services", "Logistics Solutions", "Micro Delivery Service", "Micro Mobility Vehicles", "Smart Parking", "Supply Chain Organisation", "Transportation And Logistics", "Trucking Services", "commercial passenger ", "Travel technology and distribution company "]

# Service Industry
Class53 = ["daily car wash", "parking solutions", "Senior care services", "Fire Rescue Service", "fire service", "emergency services", "Firefighters"]

# Marine Sector
Class54 = ["maritime", "shipping", "shipbuilder", "cargo management", "navy struck", "naval ships", "sea port", "sea port", "fleet", "ships", "eastern fleet", "sea", "Naval"]

# Animal Farming
Class55 = ["fine crimped wool", "sheep", "handfeeding"]

# Government and Political Sector
Class56 = ["Uniform Civil Code" , "AIMIM chief" , "Central government" , "officials" , "PM" , "Swachh Bharat Abhiyan" , "BJP leaders" , "state department" , "embassy" ,"Government", "regulatory commission", "bureau"]

classes = Class1  + Class2 + Class3 + Class4 + Class5 + Class6 + Class7 + Class8 + Class9 + Class10 + Class11 + Class12 + Class13 + Class14 + Class15 + Class16 + Class17 + Class18 + Class19 + Class20 + Class21 + Class22 + Class23 + Class24 + Class25 + Class26 + Class27 + Class28 + Class29 + Class30 + Class31 + Class32 + Class33 + Class34 + Class35 + Class36 + Class37 + Class38 + Class39 + Class40 + Class41 + Class42 + Class43 + Class44 + Class45 + Class46 + Class47 + Class48 + Class49 + Class50 + Class51 + Class52 + Class53 + Class54 + Class55 + Class56

vectorizer = CountVectorizer(binary=True)
train_x_vectors = vectorizer.fit_transform(classes)

class Category:
  label1 = "Banking and Finance"
  label2 = "Education and Training"
  label3 = "HealthCare & pharmaceuticals"
  label4 = "BioTechnology"
  label5 = "HealthTech"
  label6 = "Food Tech"
  label7 = "Automotive"
  label8 = "Life Science"
  label9 = "Environment tech"
  label10 = "Business Services"
  label11 = "TechWear"
  label12 = "Personal Care Products and Fashion Tech"
  label13 = "Internet and E-commerce"
  label14 = "Media and Entertainment"
  label15 = "Construction and Infrastructure"
  label16 = "Wholesale Trade & Retail"
  label17 = "Insurance"
  label18 = "Data and Analytics"
  label19 = "Aviation and Aerospace"
  label20 = "Sports and Recreation"
  label21 = "Online Dating and Matchmaking Tech"
  label22 = "Hospitality and Tourism"
  label23 = "Ride Hailing Services"
  label24 = "Textiles and Apparel"
  label25 = "Nonprofit and Philanthropy"
  label26 = "Journalism and Publishing"
  label27 = "Consumer Electronics"
  label28 = "Electronic Manufacturing"
  label29 = "Telecommunication"
  label30 = "Agriculture and Farming"
  label31 = "Oil Refining Sector"
  label32 = "Energy Sector"
  label33 = "Defence and Military"
  label34 = "Management and Consulting"
  label35 = "Gig Economy"
  label36 = "Waste Management and Recycling"
  label37 = "Manufacturing"
  label38 = "Veterinary Activities"
  label39 = "Food & Beverage"
  label40 = "Chemical Sector"
  label41 = "Import & Export Sector"
  label42 = "Industrial Manufacturing"
  label43 = "Information Technology"
  label44 = "Saas Sector"
  label45 = "SeaFood Sector"
  label46 = "Robotics and Automation"
  label47 = "Water and Sanitation"
  label48 = "Legal and Law Enforcement"
  label49 = "Medical Devices"
  label50 = "Mining and Metals"
  label51 = "DeepTech"
  label52 = "Transportation and Logistics Tech"
  label53 = "Service Industry"
  label54 = "Marine Sector"
  label55 = "Animal Farming"
  label56 = "Government and Political Sector"

clf_svm = svm.SVC(kernel='linear', decision_function_shape='ovr',probability=True)
categry = [Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label1,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label2,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label3,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label4,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label5,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label6,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label7,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label8,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label9,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label10,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label11,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label12,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label13,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label14,Category.label15,Category.label15,Category.label15,Category.label15,Category.label15,Category.label15,Category.label15,Category.label15,Category.label15,Category.label15,Category.label15,Category.label15,Category.label15,Category.label15,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label16,Category.label17,Category.label17,Category.label17,Category.label17,Category.label17,Category.label17,Category.label17,Category.label17,Category.label17,Category.label17,Category.label17,Category.label17,Category.label17,Category.label17,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label18,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label19,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label20,Category.label21,Category.label21,Category.label21,Category.label21,Category.label21,Category.label21,Category.label21,Category.label21,Category.label21,Category.label21,Category.label21,Category.label21,Category.label21,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label22,Category.label23,Category.label23,Category.label23,Category.label23,Category.label23,Category.label23,Category.label23,Category.label23,Category.label23,Category.label23,Category.label23,Category.label23,Category.label23,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label24,Category.label25,Category.label25,Category.label25,Category.label25,Category.label25,Category.label25,Category.label25,Category.label26,Category.label26,Category.label26,Category.label26,Category.label26,Category.label26,Category.label26,Category.label26,Category.label26,Category.label26,Category.label26,Category.label27,Category.label27,Category.label27,Category.label27,Category.label27,Category.label27,Category.label27,Category.label27,Category.label27,Category.label27,Category.label27,Category.label27,Category.label27,Category.label27,Category.label27,Category.label28,Category.label28,Category.label28,Category.label28,Category.label28,Category.label28,Category.label28,Category.label28,Category.label28,Category.label28,Category.label28,Category.label28,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label29,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label30,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label31,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label32,Category.label33,Category.label33,Category.label33,Category.label33,Category.label33,Category.label33,Category.label33,Category.label33,Category.label33,Category.label33,Category.label33,Category.label34,Category.label34,Category.label34,Category.label34,Category.label34,Category.label34,Category.label34,Category.label34,Category.label34,Category.label34,Category.label34,Category.label34,Category.label34,Category.label34,Category.label35,Category.label35,Category.label35,Category.label35,Category.label35,Category.label35,Category.label35,Category.label35,Category.label35,Category.label35,Category.label36,Category.label36,Category.label36,Category.label36,Category.label36,Category.label36,Category.label36,Category.label36,Category.label37,Category.label37,Category.label37,Category.label37,Category.label37,Category.label37,Category.label37,Category.label38,Category.label38,Category.label38,Category.label38,Category.label38,Category.label38,Category.label38,Category.label38,Category.label39,Category.label39,Category.label39,Category.label39,Category.label39,Category.label39,Category.label39,Category.label40,Category.label40,Category.label40,Category.label40,Category.label40,Category.label40,Category.label41,Category.label41,Category.label41,Category.label41,Category.label41,Category.label41,Category.label41,Category.label41,Category.label42,Category.label42,Category.label42,Category.label42,Category.label42,Category.label42,Category.label42,Category.label43,Category.label43,Category.label43,Category.label43,Category.label43,Category.label43,Category.label43,Category.label43,Category.label43,Category.label43,Category.label43,Category.label43,Category.label43,Category.label43,Category.label44,Category.label44,Category.label44,Category.label44,Category.label44,Category.label44,Category.label44,Category.label44,Category.label44,Category.label44,Category.label45,Category.label45,Category.label45,Category.label45,Category.label45,Category.label45,Category.label45,Category.label45,Category.label46,Category.label46,Category.label46,Category.label46,Category.label46,Category.label46,Category.label46,Category.label46,Category.label46,Category.label46,Category.label46,Category.label46,Category.label47,Category.label47,Category.label47,Category.label48,Category.label48,Category.label48,Category.label48,Category.label48,Category.label48,Category.label48,Category.label49,Category.label49,Category.label49,Category.label49,Category.label49,Category.label49,Category.label50,Category.label50,Category.label50,Category.label50,Category.label51,Category.label51,Category.label51,Category.label51,Category.label51,Category.label51,Category.label51,Category.label51,Category.label51,Category.label51,Category.label51,Category.label51,Category.label51,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label52,Category.label53,Category.label53,Category.label53,Category.label53,Category.label53,Category.label53,Category.label53,Category.label54,Category.label54,Category.label54,Category.label54,Category.label54,Category.label54,Category.label54,Category.label54,Category.label54,Category.label54,Category.label54,Category.label54,Category.label54,Category.label55,Category.label55,Category.label55,Category.label56,Category.label56,Category.label56,Category.label56,Category.label56,Category.label56,Category.label56,Category.label56,Category.label56,Category.label56,Category.label56,Category.label56]

clf_svm.fit(train_x_vectors, categry)

from transformers import AutoModelForQuestionAnswering, AutoTokenizer, pipeline
model = AutoModelForQuestionAnswering.from_pretrained('deepset/roberta-base-squad2')
tokenizer = AutoTokenizer.from_pretrained('deepset/roberta-base-squad2')
nlp_qa = pipeline('question-answering', model='deepset/roberta-base-squad2', tokenizer='deepset/roberta-base-squad2')

def ic_picked(article):
    nlp = spacy.load("en_core_web_lg")
    matcher = Matcher(nlp.vocab)

    pattern = [{"POS": "ADJ", "OP": "?"}, {"POS": "NOUN"}, {"LOWER": {"IN": ["brand","chain","giant","providers","provider","firm","start-up","company", "startup", "platform","app","services","start-up"]}}]
    matcher.add("SPECIFIC_PHRASE", [pattern])
    doc = nlp(article)

    matches = matcher(doc)

    matched_texts = []
    for match_id, start, end in matches:
        matched_text = doc[start:end].text
        matched_texts.append(matched_text)

    return matched_texts

def find_matching_word(ic_picked_word):
    words_to_check = ["brand","chain","giant","providers","provider","firm","start-up","company", "startup", "platform","app","services","start-up"]
    ic_picked_word = ic_picked_word.lower()
    for word in words_to_check:
        if word in ic_picked_word:
            return word
    return None

def find_sentence(paragraph, word):
    sentences = paragraph.split('.')
    for sentence in sentences:
        if word.lower() in sentence.lower():
            return sentence.strip()
    return None

def last_word(final_word):
    words = final_word.split()
    if len(words) > 0:
        return words[-1]
    else:
        return ""

def confirmed_ic_picked(content, pick):
    if not content or not pick:
        return "", ""
    context = find_sentence(content, pick)
    if context is None:
        return "", ""
    SCORE_THRESHOLD = 0.01

    question = f"{pick} is which company"
    QA_input = {'question': question, 'context': context}
    res = nlp_qa(QA_input)
    picked_org = res['answer'] if res.get('score', 0) >= SCORE_THRESHOLD else ''

    word = find_matching_word(pick)
    question = f"{picked_org} is which {word}"
    QA_input = {'question': question, 'context': context}
    res = nlp_qa(QA_input)
    plucked = res['answer'] if res.get('score', 0) >= SCORE_THRESHOLD else ''
    pluck = f"{plucked} {word}"
    key_value_pair = (picked_org, pluck)
    org_picked, ic_picked = key_value_pair
    org_picked = org_picked.replace("\n", "")
    org_picked = org_picked.replace("\n\n", "")
    ic_picked = ic_picked.replace("\n", "")
    ic_picked = ic_picked.replace("\n\n", "")
    return org_picked, ic_picked

def confirmed_ic_extracted(content, extract):
    if not content or not extract:
        return "", ""
    context = find_sentence(content, extract)
    if context is None:
        return "", ""

    SCORE_THRESHOLD = 0.01
    question = f"{extract} is which company"
    QA_input = {'question': question, 'context': context}
    res = nlp_qa(QA_input)
    org_extracted = res['answer'] if res.get('score', 0) >= SCORE_THRESHOLD else ''
    word = last_word(extract)
    question = f"{org_extracted} is which {word}"
    QA_input = {'question': question, 'context': context}
    res = nlp_qa(QA_input)
    mined = res['answer'] if res.get('score', 0) >= SCORE_THRESHOLD else ''
    key_value_pair = (org_extracted, mined)
    org_extracted, mined = key_value_pair
    org_extracted = org_extracted.replace("\n", "")
    org_extracted = org_extracted.replace("\n\n", "")
    mined = mined.replace("\n", "")
    mined = mined.replace("\n\n", "")
    return org_extracted, mined

def factacy_prediction(text):
    pre_text = vectorizer.transform([text])
    boe = clf_svm.predict(pre_text)
    class_probabilities = clf_svm.predict_proba(pre_text)
    confidence_threshold = 0.13
    classes = clf_svm.classes_
    max_accuracy = confidence_threshold
    predicted_category = None
    for class_idx, prob in enumerate(class_probabilities[0]):
        if prob >= confidence_threshold and prob > max_accuracy:
            max_accuracy = prob
            predicted_category = classes[class_idx]
    return predicted_category

def ic_process(content):
    first_pick = ic_picked(content)
    pick_set = set(first_pick)
    picked = list(pick_set)
    doc = nlp_ic(content)
    first_extract = []
    for ent in doc.ents:
        if ent.label_ == 'INDUSTRIAL CLASSIFICATION':
            first_extract.append(ent.text)
    extract_set = set(first_extract)
    extracted = list(extract_set)
    ic_got_list = []
    for pick in picked:
        pick_got = {}
        if len(pick) > 0:
            org_pick, ic_pick = confirmed_ic_picked(content, pick)
            predict = factacy_prediction(ic_pick)
            org_check = org_pick.lower()
            confirmed_org = None
            confirmed_org = org_check
            pick_got["org"] = confirmed_org
            pick_got["ic_picked"] = ic_pick
            pick_got["factacy_classification"] = predict
            if ic_pick and not predict:
              pick_got["Manual Check"] = "Imporve bag of words"
            ic_got_list.append(pick_got)
    for extract in extracted:
        extract_got = {}
        if len(extract) > 0:
            org_extract, mined = confirmed_ic_extracted(content, extract)
            predict = factacy_prediction(mined)
            org_check = org_extract.lower()
            confirmed_org = None
            confirmed_org = org_check
            extract_got["org"] = confirmed_org
            extract_got["ic_extracted"] = mined
            extract_got["factacy_classification"] = predict
            if mined and not predict:
              extract_got["Manual Check"] = "Imporve bag of words"
            ic_got_list.append(extract_got)
    return ic_got_list

def data_formating(lf):
    final_df = pd.DataFrame(columns=['Art_Id', 'Ic_result', 'org', 'ic_picked', 'ic_extracted', 'factacy_classification', 'org_manual_check', 'published_date'])
    for index, row in lf.iterrows():
        Art_ID = row['Art_Id']
        IC = row['Ic_result']
        date = row["published_date"]
        for ic_dict in IC:
            new_row = pd.DataFrame({
                'Art_Id': Art_ID,
                'ic': IC,
                'org': ic_dict.get('org', ''),
                'ic_picked': ic_dict.get('ic_picked', ''),
                'ic_extracted': ic_dict.get('ic_extracted', ''),
                'factacy_classification': ic_dict.get('factacy_classification', ''),
                'org_manual_check': ic_dict.get('org_manual_check', ''),
                'published_date': date
            })
            final_df = pd.concat([final_df, new_row], ignore_index=True)
    final_df["ic_got"] = final_df["ic_picked"].fillna("") + " " + final_df["ic_extracted"].fillna("")
    final_df = final_df.rename(columns={"factacy_classification": "Factacy_BOW_Classification"})
    final_df = final_df[['Art_Id', 'org','ic_got', 'Factacy_BOW_Classification','published_date']]
    final_df = final_df.drop_duplicates(subset=['Art_Id', 'org','ic_got', 'Factacy_BOW_Classification', 'published_date'], keep='first')
    return final_df

lf = pd.DataFrame(columns = ["Art_Id","Ic_result","published_date"])
for idx, row in new_df.iterrows():
    ID, content = row["id"], row["content_cl"]
    date = row["published_date"]
    ic_result = ic_process(content)
    entry = pd.DataFrame({"Art_Id":[ID],"Ic_result":[ic_result],"published_date":[date]})
    lf = pd.concat([lf, entry], ignore_index = True)

final_df = data_formating(lf)

sector["text"] = sector["Subsector level2"].fillna('') + " " + sector["Subsector level3"].fillna('') + " " + sector["Keywords"].fillna('')
sector["text"] = sector["text"].fillna('')
sector["text"] = sector["text"].str.lower().replace('[^a-zA-Z0-9]', ' ', regex=True)
sector["text"] = sector["text"].apply(lambda x: word_tokenize(str(x)))
lemmatizer = WordNetLemmatizer()
sector["text"] = sector["text"].apply(lambda x: [lemmatizer.lemmatize(word) for word in x])
stop_words = set(stopwords.words("english"))
sector["text"] = sector["text"].apply(lambda x: [word for word in x if word not in stop_words])
sector["text"] = sector["text"].apply(lambda x: ' '.join(x))
vectorizer = CountVectorizer()
X = vectorizer.fit_transform(sector['text'])
y = sector[['Main Sector', 'Subsector level1', 'Subsector level2', 'Subsector level3']].apply(lambda x: '_'.join(map(str, x)), axis=1)
classifier = MultinomialNB()
classifier.fit(X, y)
ic_got = final_df["ic_got"]
published_date = final_df["published_date"]
ic_got.fillna('', inplace=True)
X_new = vectorizer.transform(ic_got)
probabilities = classifier.predict_proba(X_new)
predicted_classes = classifier.classes_
predicted_indices = [list(predicted_classes).index(prediction) for prediction in classifier.predict(X_new)]
predicted_probabilities = [prob[predicted_index] for prob, predicted_index in zip(probabilities, predicted_indices)]
trail_df = pd.DataFrame({
    'Art_Id': final_df['Art_Id'],
    'org': final_df["org"],
    'ic_got': ic_got,
    "Factacy_BOW_Classification": final_df["Factacy_BOW_Classification"],
    'Published_Date': published_date,
    'Predicted': classifier.predict(X_new),
    'Probability': predicted_probabilities
})
trail_df[['factacy_main_sector', "factacy_subsector1", "factacy_subsector2", "factacy_subsector3"]] = trail_df['Predicted'].str.split('_', expand=True)
trail_df = trail_df.drop(columns = ["Predicted"])

COSMOS_DB_URI = 'https://cdb-universe.documents.azure.com:443/'
COSMOS_DB_KEY = 'hp2IylctlhLVUNKF1x6cFzNaZUR5zduVVWbutCtHyL15v4P6MpkYxBLNXU17h4FnQaYxhMOHiHKCACDbVzQN9A=='
DATABASE_NAME = 'cdb-L1'
CONTAINER_NAME = 'org-ic'

client = CosmosClient(COSMOS_DB_URI, credential=COSMOS_DB_KEY)
error_records = []
try:
    container = client.get_database_client(DATABASE_NAME).get_container_client(CONTAINER_NAME)

    json_data = trail_df.to_json(orient='records')
    json_data = json.loads(json_data)

    for item in json_data:
        try:
            query = 'SELECT * FROM c WHERE c.org = @org AND c.Art_Id = @Art_Id'

            query_params = [
                {"name": "@org", "value": item["org"]},
                {"name": "@Art_Id", "value": item["Art_Id"]}
            ]

            result = list(
                container.query_items(
                    query,
                    parameters=query_params,
                    enable_cross_partition_query=True
                )
            )

            if not result:
                record = {
                    "Art_Id": item.get("Art_Id"),
                    "org": item.get("org"),
                    "Factacy_BOW_Classification": item.get("Factacy_BOW_Classification"),
                    "ic_got": item.get("ic_got"),
                    "factacy_main_sector": item.get("factacy_main_sector"),
                    "factacy_subsector1" : item.get("factacy_subsector1"),
                    "factacy_subsector2" : item.get("factacy_subsector2"),
                    "factacy_subsector3" : item.get("factacy_subsector3"),
                    "probability" : item.get("Probability"),
                    "published_date" : item.get("published_date")
                }
                new_id = str(uuid.uuid4())
                record["id"] = new_id

                container.create_item(body=record)
                print(f"Insert successful! {record['org']}")
            else:
                print(f"Skipping existing record with Company Name: {item['org']}")
        except Exception as e:
            error_records.append({"record": item, "error": str(e)})
            print(f"Error processing record: {item}, Error: {e}")
except exceptions.CosmosHttpResponseError as e:
    print(f"Failed to upsert. Error: {e}")

