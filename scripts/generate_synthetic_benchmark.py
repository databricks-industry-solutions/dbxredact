#!/usr/bin/env python3
"""Generate fully synthetic benchmark data for dbxredact evaluation.

Usage:
    python scripts/generate_synthetic_benchmark.py                   # both domains
    python scripts/generate_synthetic_benchmark.py --domain medical   # medical only
    python scripts/generate_synthetic_benchmark.py --domain finance   # finance only

Labels used (clear PII only):
    NAME, DATE, LOCATION, IDNUM, CONTACT
"""
import argparse, csv, os, sys

# ═══════════════════════════════════════════════════════════════════════════
# MEDICAL DOCUMENTS  (doc_id 1-10)
# ═══════════════════════════════════════════════════════════════════════════

MEDICAL_DOCUMENTS = []

# ── 1: Discharge Summary ─────────────────────────────────────────────────
MEDICAL_DOCUMENTS.append((1, """\
847291053
Riverside General Hospital
Patient: Sarah Mitchell
MRN: 55412
Admission Date: 04/15/2023
Discharge Date: 04/22/2023

DISCHARGE SUMMARY

Sarah Mitchell is a female admitted to Riverside General Hospital on 04/15/2023 with acute cholecystitis. She was evaluated by James Rivera in the emergency department. Laparoscopic cholecystectomy was performed on 04/17/2023 without complications. The patient tolerated the procedure well and was advanced to a regular diet by postoperative day two.

Laboratory results on 04/20/2023 showed normalization of liver enzymes. The patient was discharged home in stable condition on 04/22/2023 with follow-up scheduled at Riverside General Hospital outpatient clinic.

ATTENDING PHYSICIAN: James Rivera
CONTACT: 555-867-5309
ADDRESS: 42 Oak Street, Springfield, Illinois 62704
SSN: 451-82-7731
""", [
    ("847291053", "IDNUM"),
    ("Riverside General Hospital", "LOCATION"),
    ("Sarah Mitchell", "NAME"),
    ("55412", "IDNUM"),
    ("04/15/2023", "DATE"),
    ("04/22/2023", "DATE"),
    ("Sarah Mitchell", "NAME"),
    ("Riverside General Hospital", "LOCATION"),
    ("04/15/2023", "DATE"),
    ("James Rivera", "NAME"),
    ("04/17/2023", "DATE"),
    ("04/20/2023", "DATE"),
    ("04/22/2023", "DATE"),
    ("Riverside General Hospital", "LOCATION"),
    ("James Rivera", "NAME"),
    ("555-867-5309", "CONTACT"),
    ("42 Oak Street", "LOCATION"),
    ("Springfield", "LOCATION"),
    ("Illinois", "LOCATION"),
    ("62704", "LOCATION"),
    ("451-82-7731", "IDNUM"),
]))

# ── 2: Progress Note ─────────────────────────────────────────────────────
MEDICAL_DOCUMENTS.append((2, """\
Progress Note
Date: November 3, 2024
Provider: Linda Nakamura
Facility: Cedarwood Medical Center

Patient: Robert Chambers (MRN: 7829104)
DOB: 06/18/1955

Robert Chambers presents today for follow-up of type 2 diabetes mellitus. He reports compliance with metformin 1000mg twice daily. Blood glucose log shows fasting levels between 110-140 mg/dL. HbA1c drawn on October 28, 2024 was 7.2%.

Physical exam reveals blood pressure 138/82, heart rate 76 bpm, weight 198 lbs. No peripheral edema. Monofilament exam normal bilaterally.

Assessment: Type 2 diabetes, fair control. Hypertension, controlled.

Plan: Continue current medications. Recheck HbA1c in three months. Referral to Cedarwood Medical Center Nutrition Services. Patient to call 503-441-2288 with any concerns.

Linda Nakamura
Cedarwood Medical Center
1200 Pine Boulevard, Portland, Oregon 97205
""", [
    ("November 3, 2024", "DATE"),
    ("Linda Nakamura", "NAME"),
    ("Cedarwood Medical Center", "LOCATION"),
    ("Robert Chambers", "NAME"),
    ("7829104", "IDNUM"),
    ("06/18/1955", "DATE"),
    ("Robert Chambers", "NAME"),
    ("October 28, 2024", "DATE"),
    ("Cedarwood Medical Center", "LOCATION"),
    ("503-441-2288", "CONTACT"),
    ("Linda Nakamura", "NAME"),
    ("Cedarwood Medical Center", "LOCATION"),
    ("1200 Pine Boulevard", "LOCATION"),
    ("Portland", "LOCATION"),
    ("Oregon", "LOCATION"),
    ("97205", "LOCATION"),
]))

# ── 3: Referral Letter ───────────────────────────────────────────────────
MEDICAL_DOCUMENTS.append((3, """\
Mercy Valley Oncology Center
456 Elm Avenue, Austin, Texas 78701
Phone: 512-330-7654
Fax: 512-330-7655

Date: February 10, 2025

RE: Patient Referral
Patient: Angela Pratt
DOB: 09/22/1968
MRN: 3304817

Dear Kenji Watanabe,

I am referring Angela Pratt for evaluation of a newly diagnosed left breast mass identified on mammography performed on 01/28/2025 at Austin Imaging Associates. Ultrasound-guided biopsy on 02/05/2025 confirmed invasive ductal carcinoma, grade 2.

Angela Pratt is otherwise in good health with no significant comorbidities. She has a family history of breast cancer in her mother. Please evaluate for surgical options and adjuvant therapy.

Enclosed: pathology report, imaging studies, and lab results.

Sincerely,
Marcus Delgado
Mercy Valley Oncology Center
Phone: 512-330-7654
Patient SSN: 298-44-6103
""", [
    ("Mercy Valley Oncology Center", "LOCATION"),
    ("456 Elm Avenue", "LOCATION"),
    ("Austin", "LOCATION"),
    ("Texas", "LOCATION"),
    ("78701", "LOCATION"),
    ("512-330-7654", "CONTACT"),
    ("512-330-7655", "CONTACT"),
    ("February 10, 2025", "DATE"),
    ("Angela Pratt", "NAME"),
    ("09/22/1968", "DATE"),
    ("3304817", "IDNUM"),
    ("Kenji Watanabe", "NAME"),
    ("Angela Pratt", "NAME"),
    ("01/28/2025", "DATE"),
    ("Austin Imaging Associates", "LOCATION"),
    ("02/05/2025", "DATE"),
    ("Angela Pratt", "NAME"),
    ("Marcus Delgado", "NAME"),
    ("Mercy Valley Oncology Center", "LOCATION"),
    ("512-330-7654", "CONTACT"),
    ("298-44-6103", "IDNUM"),
]))

# ── 4: Lab Report ────────────────────────────────────────────────────────
MEDICAL_DOCUMENTS.append((4, """\
LABORATORY REPORT
Pinecrest Diagnostics Laboratory
800 Birch Lane, Denver, Colorado 80202

Patient: Felicia Okonkwo
MRN: 6601938
DOB: 12/04/1982
Collection Date: 08/19/2024
Reporting Date: 08/20/2024
Ordering Physician: Thomas Brennan

COMPLETE BLOOD COUNT
WBC: 6.8 x10^3/uL (4.5-11.0)
RBC: 4.52 x10^6/uL (4.0-5.5)
Hemoglobin: 13.1 g/dL (12.0-16.0)
Hematocrit: 39.2% (36-46)
Platelets: 245 x10^3/uL (150-400)

COMPREHENSIVE METABOLIC PANEL
Glucose: 102 mg/dL (70-100) H
BUN: 18 mg/dL (7-20)
Creatinine: 0.9 mg/dL (0.6-1.2)
Sodium: 141 mEq/L (136-145)
Potassium: 4.1 mEq/L (3.5-5.0)
AST: 22 U/L (10-40)
ALT: 28 U/L (7-56)

Verified by: Patricia Huang
Pinecrest Diagnostics Laboratory
Results faxed to 303-555-0142
Patient contact: felicia.okonkwo@email.com
Account: 9920-4471-83
""", [
    ("Pinecrest Diagnostics Laboratory", "LOCATION"),
    ("800 Birch Lane", "LOCATION"),
    ("Denver", "LOCATION"),
    ("Colorado", "LOCATION"),
    ("80202", "LOCATION"),
    ("Felicia Okonkwo", "NAME"),
    ("6601938", "IDNUM"),
    ("12/04/1982", "DATE"),
    ("08/19/2024", "DATE"),
    ("08/20/2024", "DATE"),
    ("Thomas Brennan", "NAME"),
    ("Patricia Huang", "NAME"),
    ("Pinecrest Diagnostics Laboratory", "LOCATION"),
    ("303-555-0142", "CONTACT"),
    ("felicia.okonkwo@email.com", "CONTACT"),
    ("9920-4471-83", "IDNUM"),
]))

# ── 5: Surgical Note ─────────────────────────────────────────────────────
MEDICAL_DOCUMENTS.append((5, """\
OPERATIVE REPORT

Facility: Lakewood Surgical Center
Date of Surgery: March 8, 2024
Surgeon: David Kim
Assistant: Rachel Foster
Anesthesiologist: Omar Hassan

Patient: William Torres
MRN: 1158274
DOB: 07/30/1971

PREOPERATIVE DIAGNOSIS: Right inguinal hernia
POSTOPERATIVE DIAGNOSIS: Right inguinal hernia

PROCEDURE: Laparoscopic right inguinal hernia repair with mesh

William Torres was brought to the operating room at Lakewood Surgical Center and placed in supine position. General anesthesia was administered by Omar Hassan. A standard three-port laparoscopic approach was used. The hernia defect was identified and reduced. A 10x15 cm polypropylene mesh was placed and secured with absorbable tacks.

Estimated blood loss was minimal. No complications. William Torres was transferred to recovery in stable condition.

David Kim
Lakewood Surgical Center
Phone: 720-889-3344
Patient SSN: 612-33-9087
""", [
    ("Lakewood Surgical Center", "LOCATION"),
    ("March 8, 2024", "DATE"),
    ("David Kim", "NAME"),
    ("Rachel Foster", "NAME"),
    ("Omar Hassan", "NAME"),
    ("William Torres", "NAME"),
    ("1158274", "IDNUM"),
    ("07/30/1971", "DATE"),
    ("William Torres", "NAME"),
    ("Lakewood Surgical Center", "LOCATION"),
    ("Omar Hassan", "NAME"),
    ("William Torres", "NAME"),
    ("David Kim", "NAME"),
    ("Lakewood Surgical Center", "LOCATION"),
    ("720-889-3344", "CONTACT"),
    ("612-33-9087", "IDNUM"),
]))

# ── 6: Radiology Report ──────────────────────────────────────────────────
MEDICAL_DOCUMENTS.append((6, """\
RADIOLOGY REPORT

Facility: Maplewood Imaging Center
Patient: Diana Kowalski
MRN: 4420561
DOB: 03/15/1979
Exam Date: June 12, 2024
Referring Physician: Steven Park

EXAM: CT Abdomen and Pelvis with contrast

CLINICAL INDICATION: Abdominal pain, rule out appendicitis

TECHNIQUE: Axial images of the abdomen and pelvis were obtained following administration of oral and intravenous contrast.

FINDINGS: The appendix measures 4mm in diameter and is normal in appearance. No periappendiceal fat stranding or fluid. The liver, spleen, pancreas, and adrenal glands are unremarkable. Kidneys are normal in size and enhancement. No hydronephrosis. No free fluid. No pathologically enlarged lymph nodes.

IMPRESSION: Normal CT abdomen and pelvis. No evidence of appendicitis.

Interpreted by: Yuki Tanaka
Maplewood Imaging Center
2900 Maple Drive, Charlotte, North Carolina 28202
Report sent to Steven Park at steven.park@healthcare.org
""", [
    ("Maplewood Imaging Center", "LOCATION"),
    ("Diana Kowalski", "NAME"),
    ("4420561", "IDNUM"),
    ("03/15/1979", "DATE"),
    ("June 12, 2024", "DATE"),
    ("Steven Park", "NAME"),
    ("Yuki Tanaka", "NAME"),
    ("Maplewood Imaging Center", "LOCATION"),
    ("2900 Maple Drive", "LOCATION"),
    ("Charlotte", "LOCATION"),
    ("North Carolina", "LOCATION"),
    ("28202", "LOCATION"),
    ("Steven Park", "NAME"),
    ("steven.park@healthcare.org", "CONTACT"),
]))

# ── 7: Consultation Note ─────────────────────────────────────────────────
MEDICAL_DOCUMENTS.append((7, """\
CONSULTATION NOTE

Date: 09/14/2024
Requesting Physician: Natalie Bergman
Consultant: Alejandro Reyes, Cardiology
Facility: Summit Heart Institute

Patient: Gregory Lawson
MRN: 8837102
DOB: 11/27/1960
SSN: 377-50-2184

REASON FOR CONSULTATION: Evaluation of newly detected atrial fibrillation

Gregory Lawson is a male who was found to have atrial fibrillation on routine ECG performed on 09/10/2024 at Summit Heart Institute. He denies palpitations, chest pain, or dyspnea. Past medical history includes hypertension and hyperlipidemia.

Physical examination reveals irregularly irregular heart rhythm at a rate of 82 bpm. No murmurs, gallops, or rubs. Lungs clear. No peripheral edema.

ECG confirms atrial fibrillation with controlled ventricular rate. Echocardiogram from September 12, 2024 shows preserved ejection fraction of 58%, left atrial enlargement, and no valvular abnormalities.

RECOMMENDATIONS: Start apixaban 5mg twice daily for stroke prophylaxis. Rate control with metoprolol 25mg twice daily. Follow-up with Alejandro Reyes in four weeks. Contact 214-778-5501 for scheduling.

Alejandro Reyes
Summit Heart Institute
5500 Highland Road, Dallas, Texas 75201
""", [
    ("09/14/2024", "DATE"),
    ("Natalie Bergman", "NAME"),
    ("Alejandro Reyes", "NAME"),
    ("Summit Heart Institute", "LOCATION"),
    ("Gregory Lawson", "NAME"),
    ("8837102", "IDNUM"),
    ("11/27/1960", "DATE"),
    ("377-50-2184", "IDNUM"),
    ("Gregory Lawson", "NAME"),
    ("09/10/2024", "DATE"),
    ("Summit Heart Institute", "LOCATION"),
    ("September 12, 2024", "DATE"),
    ("Alejandro Reyes", "NAME"),
    ("214-778-5501", "CONTACT"),
    ("Alejandro Reyes", "NAME"),
    ("Summit Heart Institute", "LOCATION"),
    ("5500 Highland Road", "LOCATION"),
    ("Dallas", "LOCATION"),
    ("Texas", "LOCATION"),
    ("75201", "LOCATION"),
]))

# ── 8: Emergency Department Note ─────────────────────────────────────────
MEDICAL_DOCUMENTS.append((8, """\
EMERGENCY DEPARTMENT NOTE

Facility: Bayshore Community Hospital
Date: 01/05/2025
Provider: Christine Adler

Patient: Marcus Johnson
MRN: 2219467
DOB: 08/03/1990

CHIEF COMPLAINT: Laceration to left forearm

Marcus Johnson presents to the emergency department at Bayshore Community Hospital after sustaining a laceration to the left forearm from a broken glass approximately one hour ago. He denies numbness, weakness, or tendon involvement. Tetanus is current.

EXAMINATION: There is a 4 cm linear laceration over the volar aspect of the left forearm. No tendon or nerve involvement. Sensation and motor function intact distally. Wound is clean with no foreign body.

TREATMENT: Wound irrigated with sterile saline. Repair performed with 4-0 nylon, simple interrupted sutures. Wound dressed with bacitracin and sterile gauze.

DISPOSITION: Discharged home on 01/05/2025. Suture removal in 10 days at Bayshore Community Hospital outpatient clinic. Emergency contact: 415-209-8836.

Christine Adler
Bayshore Community Hospital
100 Harbor Way, San Francisco, California 94102
Patient ID: 8841-2025-0105
""", [
    ("Bayshore Community Hospital", "LOCATION"),
    ("01/05/2025", "DATE"),
    ("Christine Adler", "NAME"),
    ("Marcus Johnson", "NAME"),
    ("2219467", "IDNUM"),
    ("08/03/1990", "DATE"),
    ("Marcus Johnson", "NAME"),
    ("Bayshore Community Hospital", "LOCATION"),
    ("01/05/2025", "DATE"),
    ("Bayshore Community Hospital", "LOCATION"),
    ("415-209-8836", "CONTACT"),
    ("Christine Adler", "NAME"),
    ("Bayshore Community Hospital", "LOCATION"),
    ("100 Harbor Way", "LOCATION"),
    ("San Francisco", "LOCATION"),
    ("California", "LOCATION"),
    ("94102", "LOCATION"),
    ("8841-2025-0105", "IDNUM"),
]))

# ── 9: Pathology Report ──────────────────────────────────────────────────
MEDICAL_DOCUMENTS.append((9, """\
SURGICAL PATHOLOGY REPORT

Facility: Crescent Valley Medical Center
Patient: Elena Vasquez
MRN: 5504219
DOB: 05/11/1974
Date of Procedure: 10/22/2024
Date of Report: 10/25/2024
Surgeon: Philip Grant
Pathologist: Miriam Cheng

SPECIMEN: Excisional biopsy, right breast lesion

GROSS DESCRIPTION: Received fresh labeled "right breast mass" is an irregularly shaped tan-pink tissue fragment measuring 3.2 x 2.8 x 1.5 cm. A firm area measuring 1.1 cm in greatest dimension is identified centrally. Specimen entirely submitted in cassettes A1-A4.

MICROSCOPIC DESCRIPTION: Sections show invasive ductal carcinoma, Nottingham grade 2 (tubule formation 3, nuclear pleomorphism 2, mitotic count 1). Tumor measures 1.0 cm. Margins are negative with closest margin at 0.3 cm. No lymphovascular invasion. No ductal carcinoma in situ component identified.

DIAGNOSIS: Right breast, excisional biopsy: Invasive ductal carcinoma, grade 2, measuring 1.0 cm. Margins negative.

Report verified by Miriam Cheng on 10/25/2024.

Crescent Valley Medical Center
7700 Valley Parkway, Phoenix, Arizona 85004
Copies sent to Philip Grant and Elena Vasquez primary care at 602-444-9127.
""", [
    ("Crescent Valley Medical Center", "LOCATION"),
    ("Elena Vasquez", "NAME"),
    ("5504219", "IDNUM"),
    ("05/11/1974", "DATE"),
    ("10/22/2024", "DATE"),
    ("10/25/2024", "DATE"),
    ("Philip Grant", "NAME"),
    ("Miriam Cheng", "NAME"),
    ("Miriam Cheng", "NAME"),
    ("10/25/2024", "DATE"),
    ("Crescent Valley Medical Center", "LOCATION"),
    ("7700 Valley Parkway", "LOCATION"),
    ("Phoenix", "LOCATION"),
    ("Arizona", "LOCATION"),
    ("85004", "LOCATION"),
    ("Philip Grant", "NAME"),
    ("Elena Vasquez", "NAME"),
    ("602-444-9127", "CONTACT"),
]))

# ── 10: Medication Reconciliation ─────────────────────────────────────────
MEDICAL_DOCUMENTS.append((10, """\
MEDICATION RECONCILIATION

Facility: Brookfield Community Health Center
Date: 07/01/2024
Provider: Janet Owens
Patient: Daniel Nguyen
MRN: 9038715
DOB: 02/28/1987
Phone: 617-332-5540

CURRENT MEDICATIONS:
1. Lisinopril 20mg PO daily - hypertension
2. Atorvastatin 40mg PO at bedtime - hyperlipidemia
3. Metformin 500mg PO twice daily - type 2 diabetes
4. Omeprazole 20mg PO daily - GERD
5. Sertraline 50mg PO daily - depression

ALLERGIES: Penicillin (rash), Sulfa drugs (hives)

Daniel Nguyen reports compliance with all medications. No new medications, supplements, or over-the-counter drugs. Last refill at Brookfield Community Health Center pharmacy on 06/15/2024.

Reconciliation performed by Janet Owens. Next review scheduled for January 1, 2025 at Brookfield Community Health Center. Patient can reach the pharmacy at 617-332-5541 or email pharmacy@brookfieldchc.org.

Janet Owens
Brookfield Community Health Center
350 Main Street, Boston, Massachusetts 02108
Patient account: 7744-0389
""", [
    ("Brookfield Community Health Center", "LOCATION"),
    ("07/01/2024", "DATE"),
    ("Janet Owens", "NAME"),
    ("Daniel Nguyen", "NAME"),
    ("9038715", "IDNUM"),
    ("02/28/1987", "DATE"),
    ("617-332-5540", "CONTACT"),
    ("Daniel Nguyen", "NAME"),
    ("Brookfield Community Health Center", "LOCATION"),
    ("06/15/2024", "DATE"),
    ("Janet Owens", "NAME"),
    ("January 1, 2025", "DATE"),
    ("Brookfield Community Health Center", "LOCATION"),
    ("617-332-5541", "CONTACT"),
    ("pharmacy@brookfieldchc.org", "CONTACT"),
    ("Janet Owens", "NAME"),
    ("Brookfield Community Health Center", "LOCATION"),
    ("350 Main Street", "LOCATION"),
    ("Boston", "LOCATION"),
    ("Massachusetts", "LOCATION"),
    ("02108", "LOCATION"),
    ("7744-0389", "IDNUM"),
]))

# ═══════════════════════════════════════════════════════════════════════════
# FINANCE DOCUMENTS  (doc_id 101-110)
# ═══════════════════════════════════════════════════════════════════════════

FINANCE_DOCUMENTS = []

# ── 101: Account Opening Application ─────────────────────────────────────
FINANCE_DOCUMENTS.append((101, """\
NEW ACCOUNT APPLICATION

Branch: Granite Peak National Bank
1450 Commerce Street, Chicago, Illinois 60601
Date: 03/22/2024

Applicant: Priya Kapoor
Date of Birth: 07/14/1988
SSN: 319-72-4458
Phone: 312-555-8821
Email: priya.kapoor@inbox.com
Address: 2711 Lakeshore Drive, Chicago, Illinois 60614

Employment: Horizon Analytics Group
Position: Senior Data Analyst
Work Phone: 312-555-9100

Account Type: Premier Checking
Initial Deposit: $5,000.00
Funding Source: Transfer from account 4820-1193-7756 at First National Savings

Priya Kapoor certifies that all information provided is accurate and complete. Application reviewed and approved by Nathan Cole on 03/22/2024.

Branch Manager: Nathan Cole
Granite Peak National Bank
Reference: APP-2024-03-8821
""", [
    ("Granite Peak National Bank", "LOCATION"),
    ("1450 Commerce Street", "LOCATION"),
    ("Chicago", "LOCATION"),
    ("Illinois", "LOCATION"),
    ("60601", "LOCATION"),
    ("03/22/2024", "DATE"),
    ("Priya Kapoor", "NAME"),
    ("07/14/1988", "DATE"),
    ("319-72-4458", "IDNUM"),
    ("312-555-8821", "CONTACT"),
    ("priya.kapoor@inbox.com", "CONTACT"),
    ("2711 Lakeshore Drive", "LOCATION"),
    ("Chicago", "LOCATION"),
    ("Illinois", "LOCATION"),
    ("60614", "LOCATION"),
    ("Horizon Analytics Group", "LOCATION"),
    ("312-555-9100", "CONTACT"),
    ("4820-1193-7756", "IDNUM"),
    ("First National Savings", "LOCATION"),
    ("Priya Kapoor", "NAME"),
    ("Nathan Cole", "NAME"),
    ("03/22/2024", "DATE"),
    ("Nathan Cole", "NAME"),
    ("Granite Peak National Bank", "LOCATION"),
    ("APP-2024-03-8821", "IDNUM"),
]))

# ── 102: Wire Transfer Request ───────────────────────────────────────────
FINANCE_DOCUMENTS.append((102, """\
WIRE TRANSFER AUTHORIZATION

Date: August 15, 2024
Originator: Victor Stanhope
Account: 7734-0082-5519
Bank: Pacific Commerce Bank
Branch: 600 Market Street, San Francisco, California 94105

Beneficiary: Lena Morozova
Beneficiary Account: 5501-3847-2290
Beneficiary Bank: Atlantic Federal Credit Union
Routing Number: 021000089
Beneficiary Address: 88 Wall Street, New York, New York 10005

Amount: $47,250.00
Purpose: Real estate escrow deposit

Victor Stanhope authorizes Pacific Commerce Bank to debit the above account and initiate a wire transfer to the beneficiary. This request was submitted on August 15, 2024 and verified by relationship manager Diane Frost via phone at 415-662-3041.

Confirmation will be sent to victor.stanhope@proton.me.

Reference: WIRE-2024-081590
""", [
    ("August 15, 2024", "DATE"),
    ("Victor Stanhope", "NAME"),
    ("7734-0082-5519", "IDNUM"),
    ("Pacific Commerce Bank", "LOCATION"),
    ("600 Market Street", "LOCATION"),
    ("San Francisco", "LOCATION"),
    ("California", "LOCATION"),
    ("94105", "LOCATION"),
    ("Lena Morozova", "NAME"),
    ("5501-3847-2290", "IDNUM"),
    ("Atlantic Federal Credit Union", "LOCATION"),
    ("021000089", "IDNUM"),
    ("88 Wall Street", "LOCATION"),
    ("New York", "LOCATION"),
    ("New York", "LOCATION"),
    ("10005", "LOCATION"),
    ("Victor Stanhope", "NAME"),
    ("Pacific Commerce Bank", "LOCATION"),
    ("August 15, 2024", "DATE"),
    ("Diane Frost", "NAME"),
    ("415-662-3041", "CONTACT"),
    ("victor.stanhope@proton.me", "CONTACT"),
    ("WIRE-2024-081590", "IDNUM"),
]))

# ── 103: Loan Approval Letter ────────────────────────────────────────────
FINANCE_DOCUMENTS.append((103, """\
LOAN APPROVAL NOTIFICATION

Heritage Trust Bank
900 Peachtree Street, Atlanta, Georgia 30309
Phone: 404-555-7100

Date: 05/10/2024

Dear Tomoko Hayashi,

We are pleased to inform you that your application for a personal loan has been approved. The details are as follows:

Borrower: Tomoko Hayashi
SSN: 482-91-3307
Loan Number: LN-2024-047821
Loan Amount: $35,000.00
Interest Rate: 6.99% APR
Term: 60 months
Monthly Payment: $693.42
First Payment Due: 07/01/2024

Funds will be deposited into your account 3318-7720-4156 at Heritage Trust Bank on or before 05/15/2024.

For questions regarding your loan, contact your loan officer, Bradley Whitmore, at 404-555-7133 or bradley.whitmore@heritagetrust.com.

Congratulations,
Bradley Whitmore
Heritage Trust Bank
""", [
    ("Heritage Trust Bank", "LOCATION"),
    ("900 Peachtree Street", "LOCATION"),
    ("Atlanta", "LOCATION"),
    ("Georgia", "LOCATION"),
    ("30309", "LOCATION"),
    ("404-555-7100", "CONTACT"),
    ("05/10/2024", "DATE"),
    ("Tomoko Hayashi", "NAME"),
    ("Tomoko Hayashi", "NAME"),
    ("482-91-3307", "IDNUM"),
    ("LN-2024-047821", "IDNUM"),
    ("07/01/2024", "DATE"),
    ("3318-7720-4156", "IDNUM"),
    ("Heritage Trust Bank", "LOCATION"),
    ("05/15/2024", "DATE"),
    ("Bradley Whitmore", "NAME"),
    ("404-555-7133", "CONTACT"),
    ("bradley.whitmore@heritagetrust.com", "CONTACT"),
    ("Bradley Whitmore", "NAME"),
    ("Heritage Trust Bank", "LOCATION"),
]))

# ── 104: Credit Card Dispute ─────────────────────────────────────────────
FINANCE_DOCUMENTS.append((104, """\
CREDIT CARD TRANSACTION DISPUTE

Date Filed: 09/03/2024
Cardholder: Derek Olsen
Card Number (last 4): ending in 8412
Account Number: 6677-0034-8412
SSN on file: 550-18-2763

Derek Olsen is disputing the following transactions on his Evergreen Bank Visa credit card:

1. 08/27/2024 - TechMart Online - $1,249.99 - Merchandise not received
2. 08/28/2024 - GlobePay Transfer - $500.00 - Unauthorized transaction
3. 08/30/2024 - QuickShip Logistics - $89.50 - Duplicate charge

Total disputed amount: $1,839.49

Derek Olsen reports that the GlobePay Transfer charge was not authorized and may be fraudulent. He has requested a replacement card. Provisional credit has been applied pending investigation.

Case reviewed by Sandra Vega on 09/04/2024.

Sandra Vega
Evergreen Bank Dispute Resolution
2200 Central Avenue, Minneapolis, Minnesota 55401
Phone: 612-555-0088
Fax: 612-555-0089
Case Reference: DISP-2024-090334
""", [
    ("09/03/2024", "DATE"),
    ("Derek Olsen", "NAME"),
    ("6677-0034-8412", "IDNUM"),
    ("550-18-2763", "IDNUM"),
    ("Derek Olsen", "NAME"),
    ("Evergreen Bank", "LOCATION"),
    ("08/27/2024", "DATE"),
    ("08/28/2024", "DATE"),
    ("08/30/2024", "DATE"),
    ("Derek Olsen", "NAME"),
    ("Sandra Vega", "NAME"),
    ("09/04/2024", "DATE"),
    ("Sandra Vega", "NAME"),
    ("Evergreen Bank", "LOCATION"),
    ("2200 Central Avenue", "LOCATION"),
    ("Minneapolis", "LOCATION"),
    ("Minnesota", "LOCATION"),
    ("55401", "LOCATION"),
    ("612-555-0088", "CONTACT"),
    ("612-555-0089", "CONTACT"),
    ("DISP-2024-090334", "IDNUM"),
]))

# ── 105: KYC/AML Report ──────────────────────────────────────────────────
FINANCE_DOCUMENTS.append((105, """\
KNOW YOUR CUSTOMER / ANTI-MONEY LAUNDERING REVIEW

Subject: Reginald Okafor
Customer ID: CID-20190428-7731
SSN: 601-44-8829
Date of Birth: 04/28/1975
Address: 155 Beacon Street, Boston, Massachusetts 02116
Phone: 617-555-4439

Account: 8820-5567-1243 at Coastal Fidelity Bank
Account opened: 01/15/2019

REVIEW SUMMARY:
A routine AML screening on October 1, 2024 flagged account 8820-5567-1243 for unusual wire activity. Between July 1, 2024 and September 30, 2024, Reginald Okafor received 14 incoming wire transfers totaling $218,400 from three separate entities.

Source entities reviewed:
1. Northwind Trading LLC - registered in Wilmington, Delaware
2. Eastbridge Capital Partners - registered in Miami, Florida

All transactions cleared through standard channels. No OFAC matches found. Customer provided documentation supporting the transactions as consulting income.

DISPOSITION: No suspicious activity identified. Case closed on October 8, 2024.

Reviewed by: Carmen Iglesias
Compliance Department, Coastal Fidelity Bank
300 Atlantic Avenue, Boston, Massachusetts 02110
Contact: 617-555-4400 / carmen.iglesias@coastalfidelity.com
Case: AML-2024-10-0047
""", [
    ("Reginald Okafor", "NAME"),
    ("CID-20190428-7731", "IDNUM"),
    ("601-44-8829", "IDNUM"),
    ("04/28/1975", "DATE"),
    ("155 Beacon Street", "LOCATION"),
    ("Boston", "LOCATION"),
    ("Massachusetts", "LOCATION"),
    ("02116", "LOCATION"),
    ("617-555-4439", "CONTACT"),
    ("8820-5567-1243", "IDNUM"),
    ("Coastal Fidelity Bank", "LOCATION"),
    ("01/15/2019", "DATE"),
    ("October 1, 2024", "DATE"),
    ("8820-5567-1243", "IDNUM"),
    ("July 1, 2024", "DATE"),
    ("September 30, 2024", "DATE"),
    ("Reginald Okafor", "NAME"),
    ("Wilmington", "LOCATION"),
    ("Delaware", "LOCATION"),
    ("Miami", "LOCATION"),
    ("Florida", "LOCATION"),
    ("October 8, 2024", "DATE"),
    ("Carmen Iglesias", "NAME"),
    ("Coastal Fidelity Bank", "LOCATION"),
    ("300 Atlantic Avenue", "LOCATION"),
    ("Boston", "LOCATION"),
    ("Massachusetts", "LOCATION"),
    ("02110", "LOCATION"),
    ("617-555-4400", "CONTACT"),
    ("carmen.iglesias@coastalfidelity.com", "CONTACT"),
    ("AML-2024-10-0047", "IDNUM"),
]))

# ── 106: Investment Portfolio Review ──────────────────────────────────────
FINANCE_DOCUMENTS.append((106, """\
QUARTERLY INVESTMENT REVIEW

Client: Hannah Sorensen
Account: IRA-4417-2283
SSN: 224-68-5510
Advisor: Maxwell Grant
Firm: Pinnacle Wealth Advisors
Date: October 15, 2024

Dear Hannah Sorensen,

Please find your quarterly portfolio review for the period ending September 30, 2024.

PORTFOLIO SUMMARY:
Total Value: $412,837.19
Quarterly Return: +3.2%
Year-to-Date Return: +11.8%

ASSET ALLOCATION:
US Equities: 55% ($227,060)
International Equities: 15% ($61,926)
Fixed Income: 25% ($103,209)
Cash & Equivalents: 5% ($20,642)

RECOMMENDATIONS:
Given the current market environment, we recommend rebalancing the international equity allocation from 15% to 18% to capture emerging market growth. We also recommend shifting $15,000 from cash into intermediate-term bonds.

Please contact Maxwell Grant at 858-555-2210 or maxwell.grant@pinnaclewealth.com to discuss these recommendations.

Maxwell Grant
Pinnacle Wealth Advisors
4500 La Jolla Village Drive, San Diego, California 92122
""", [
    ("Hannah Sorensen", "NAME"),
    ("IRA-4417-2283", "IDNUM"),
    ("224-68-5510", "IDNUM"),
    ("Maxwell Grant", "NAME"),
    ("Pinnacle Wealth Advisors", "LOCATION"),
    ("October 15, 2024", "DATE"),
    ("Hannah Sorensen", "NAME"),
    ("September 30, 2024", "DATE"),
    ("Maxwell Grant", "NAME"),
    ("858-555-2210", "CONTACT"),
    ("maxwell.grant@pinnaclewealth.com", "CONTACT"),
    ("Maxwell Grant", "NAME"),
    ("Pinnacle Wealth Advisors", "LOCATION"),
    ("4500 La Jolla Village Drive", "LOCATION"),
    ("San Diego", "LOCATION"),
    ("California", "LOCATION"),
    ("92122", "LOCATION"),
]))

# ── 107: Mortgage Closing Document ────────────────────────────────────────
FINANCE_DOCUMENTS.append((107, """\
MORTGAGE CLOSING STATEMENT

Lender: Cornerstone Home Lending
1800 Preston Road, Dallas, Texas 75252
Loan Officer: Rachel Simmons
Phone: 972-555-6340

Borrower: Jonathan Hartwell and Megan Hartwell
Property: 312 Sycamore Lane, Frisco, Texas 75034
Loan Number: MTG-2024-112847
Closing Date: 11/15/2024

LOAN DETAILS:
Purchase Price: $485,000.00
Down Payment: $97,000.00 (20%)
Loan Amount: $388,000.00
Interest Rate: 6.625% fixed
Term: 30 years
Monthly Payment (P&I): $2,486.33

BORROWER INFORMATION:
Jonathan Hartwell - SSN: 441-29-8830
Megan Hartwell - SSN: 441-33-6652

Funds wired from escrow account 9902-6614-3381 at Lone Star Title Company on 11/15/2024.

Title insurance issued by Lone Star Title Company, 500 Main Street, Frisco, Texas 75034. Contact: 972-555-6399 or closings@lonestartitle.com.

Signed by Jonathan Hartwell and Megan Hartwell on November 15, 2024.

Notarized by: Teresa Ruiz
""", [
    ("Cornerstone Home Lending", "LOCATION"),
    ("1800 Preston Road", "LOCATION"),
    ("Dallas", "LOCATION"),
    ("Texas", "LOCATION"),
    ("75252", "LOCATION"),
    ("Rachel Simmons", "NAME"),
    ("972-555-6340", "CONTACT"),
    ("Jonathan Hartwell", "NAME"),
    ("Megan Hartwell", "NAME"),
    ("312 Sycamore Lane", "LOCATION"),
    ("Frisco", "LOCATION"),
    ("Texas", "LOCATION"),
    ("75034", "LOCATION"),
    ("MTG-2024-112847", "IDNUM"),
    ("11/15/2024", "DATE"),
    ("Jonathan Hartwell", "NAME"),
    ("441-29-8830", "IDNUM"),
    ("Megan Hartwell", "NAME"),
    ("441-33-6652", "IDNUM"),
    ("9902-6614-3381", "IDNUM"),
    ("Lone Star Title Company", "LOCATION"),
    ("11/15/2024", "DATE"),
    ("Lone Star Title Company", "LOCATION"),
    ("500 Main Street", "LOCATION"),
    ("Frisco", "LOCATION"),
    ("Texas", "LOCATION"),
    ("75034", "LOCATION"),
    ("972-555-6399", "CONTACT"),
    ("closings@lonestartitle.com", "CONTACT"),
    ("Jonathan Hartwell", "NAME"),
    ("Megan Hartwell", "NAME"),
    ("November 15, 2024", "DATE"),
    ("Teresa Ruiz", "NAME"),
]))

# ── 108: Insurance Claim ─────────────────────────────────────────────────
FINANCE_DOCUMENTS.append((108, """\
INSURANCE CLAIM SUBMISSION

Insurer: Great Lakes Mutual Insurance
Policyholder: Shannon Fitzpatrick
Policy Number: GLM-HO-2021-55843
Claim Number: CLM-2024-08-29471

Date of Loss: 08/14/2024
Date Filed: 08/18/2024
Type of Loss: Water damage - burst pipe

Shannon Fitzpatrick reports that a pipe burst in the upstairs bathroom of her residence at 741 Orchard Road, Madison, Wisconsin 53711 on 08/14/2024, causing water damage to the ceiling, flooring, and personal property on the first floor.

DAMAGE ESTIMATE:
Structural repairs: $12,400.00
Flooring replacement: $4,800.00
Personal property: $3,200.00
Temporary housing (7 nights): $1,050.00
Total estimate: $21,450.00
Deductible: $1,000.00
Net claim: $20,450.00

Adjuster assigned: Craig Dunham on 08/19/2024.
Inspection scheduled for August 22, 2024 at the property.

Craig Dunham
Great Lakes Mutual Insurance
Claims Department
Phone: 608-555-3221
Email: craig.dunham@greatlakesmutual.com

Policyholder contact: 608-555-7714 / shannon.fitz@email.com
Policyholder DOB: 11/30/1983
Policyholder SSN: 338-77-2150
""", [
    ("Great Lakes Mutual Insurance", "LOCATION"),
    ("Shannon Fitzpatrick", "NAME"),
    ("GLM-HO-2021-55843", "IDNUM"),
    ("CLM-2024-08-29471", "IDNUM"),
    ("08/14/2024", "DATE"),
    ("08/18/2024", "DATE"),
    ("Shannon Fitzpatrick", "NAME"),
    ("741 Orchard Road", "LOCATION"),
    ("Madison", "LOCATION"),
    ("Wisconsin", "LOCATION"),
    ("53711", "LOCATION"),
    ("08/14/2024", "DATE"),
    ("Craig Dunham", "NAME"),
    ("08/19/2024", "DATE"),
    ("August 22, 2024", "DATE"),
    ("Craig Dunham", "NAME"),
    ("Great Lakes Mutual Insurance", "LOCATION"),
    ("608-555-3221", "CONTACT"),
    ("craig.dunham@greatlakesmutual.com", "CONTACT"),
    ("608-555-7714", "CONTACT"),
    ("shannon.fitz@email.com", "CONTACT"),
    ("11/30/1983", "DATE"),
    ("338-77-2150", "IDNUM"),
]))

# ── 109: Tax Preparation Memo ────────────────────────────────────────────
FINANCE_DOCUMENTS.append((109, """\
TAX PREPARATION MEMORANDUM

Prepared by: Audrey Chen
Firm: Summit Tax and Advisory
820 Wilshire Boulevard, Los Angeles, California 90017
Date: March 28, 2025

Client: Raymond Dominguez and Lisa Dominguez
SSN (Raymond): 557-42-1198
SSN (Lisa): 557-49-8834
Address: 4200 Sunset Boulevard, Los Angeles, California 90029
Phone: 323-555-1847

TAX YEAR 2024 SUMMARY:

Filing Status: Married Filing Jointly
Total Income: $187,420.00
  Raymond Dominguez W-2 wages: $112,000.00
  Lisa Dominguez W-2 wages: $68,420.00
  Interest and dividends: $7,000.00

Adjustments: $6,500.00 (IRA contribution)
Standard Deduction: $29,200.00
Taxable Income: $151,720.00
Federal Tax Liability: $24,718.00
Estimated Tax Payments: $22,000.00
Balance Due: $2,718.00

State of California estimated liability: $8,410.00

Return prepared and reviewed. E-file authorization signed by Raymond Dominguez and Lisa Dominguez on March 28, 2025. Extension not required; filing deadline is April 15, 2025.

Audrey Chen
Summit Tax and Advisory
Phone: 323-555-1900
Email: audrey.chen@summittax.com
EIN: 95-4281037
""", [
    ("Audrey Chen", "NAME"),
    ("Summit Tax and Advisory", "LOCATION"),
    ("820 Wilshire Boulevard", "LOCATION"),
    ("Los Angeles", "LOCATION"),
    ("California", "LOCATION"),
    ("90017", "LOCATION"),
    ("March 28, 2025", "DATE"),
    ("Raymond Dominguez", "NAME"),
    ("Lisa Dominguez", "NAME"),
    ("557-42-1198", "IDNUM"),
    ("557-49-8834", "IDNUM"),
    ("4200 Sunset Boulevard", "LOCATION"),
    ("Los Angeles", "LOCATION"),
    ("California", "LOCATION"),
    ("90029", "LOCATION"),
    ("323-555-1847", "CONTACT"),
    ("Raymond Dominguez", "NAME"),
    ("Lisa Dominguez", "NAME"),
    ("California", "LOCATION"),
    ("Raymond Dominguez", "NAME"),
    ("Lisa Dominguez", "NAME"),
    ("March 28, 2025", "DATE"),
    ("April 15, 2025", "DATE"),
    ("Audrey Chen", "NAME"),
    ("Summit Tax and Advisory", "LOCATION"),
    ("323-555-1900", "CONTACT"),
    ("audrey.chen@summittax.com", "CONTACT"),
    ("95-4281037", "IDNUM"),
]))

# ── 110: Financial Audit Findings ─────────────────────────────────────────
FINANCE_DOCUMENTS.append((110, """\
INTERNAL AUDIT REPORT

Company: Trident Manufacturing Inc.
EIN: 36-7281445
Headquarters: 1100 Industrial Parkway, Detroit, Michigan 48201
Audit Period: January 1, 2024 through December 31, 2024
Report Date: February 20, 2025

Prepared by: Warren Blackwell
Reviewed by: Sonia Petrov

SCOPE:
This audit examined accounts payable, accounts receivable, and payroll disbursements for Trident Manufacturing Inc. for the fiscal year ending December 31, 2024.

FINDINGS:

1. PAYROLL IRREGULARITY: Employee Carlos Mendez (Employee ID: EMP-04417, SSN: 382-55-9104) received duplicate payroll deposits on June 14, 2024 and July 12, 2024 totaling $8,412.00 in excess payments. Funds were recovered on August 2, 2024 via payroll deduction.

2. VENDOR PAYMENT: A payment of $42,750.00 was issued to Apex Supply Co. on September 22, 2024 without matching purchase order. Invoice AP-2024-09-3382 has been reviewed and approved retroactively.

3. RECEIVABLE AGING: Client Westfield Dynamics (Account: AR-8827-5501) has an outstanding balance of $67,200.00 past due since October 31, 2024.

RECOMMENDATIONS:
Implement dual-authorization controls for payroll corrections. Require PO matching for all vendor payments exceeding $10,000.

Warren Blackwell
Internal Audit Department
Trident Manufacturing Inc.
Phone: 313-555-2800
Email: w.blackwell@tridentmfg.com
""", [
    ("Trident Manufacturing Inc.", "LOCATION"),
    ("36-7281445", "IDNUM"),
    ("1100 Industrial Parkway", "LOCATION"),
    ("Detroit", "LOCATION"),
    ("Michigan", "LOCATION"),
    ("48201", "LOCATION"),
    ("January 1, 2024", "DATE"),
    ("December 31, 2024", "DATE"),
    ("February 20, 2025", "DATE"),
    ("Warren Blackwell", "NAME"),
    ("Sonia Petrov", "NAME"),
    ("Trident Manufacturing Inc.", "LOCATION"),
    ("December 31, 2024", "DATE"),
    ("Carlos Mendez", "NAME"),
    ("EMP-04417", "IDNUM"),
    ("382-55-9104", "IDNUM"),
    ("June 14, 2024", "DATE"),
    ("July 12, 2024", "DATE"),
    ("August 2, 2024", "DATE"),
    ("September 22, 2024", "DATE"),
    ("AP-2024-09-3382", "IDNUM"),
    ("October 31, 2024", "DATE"),
    ("Warren Blackwell", "NAME"),
    ("Trident Manufacturing Inc.", "LOCATION"),
    ("313-555-2800", "CONTACT"),
    ("w.blackwell@tridentmfg.com", "CONTACT"),
]))


# ═══════════════════════════════════════════════════════════════════════════
# Build & write
# ═══════════════════════════════════════════════════════════════════════════

def build_rows(documents):
    """Yield (doc_id, text, begin, end, chunk, chunk_label) tuples.

    Finds ALL occurrences of each unique (chunk, label) pair in the document,
    not just the ones manually listed in annotations. This prevents phantom
    false positives from unannotated occurrences.
    """
    for doc_id, text, annotations in documents:
        unique_entities = dict()
        for chunk, label in annotations:
            unique_entities[chunk] = label

        seen = set()
        results = []
        for chunk, label in unique_entities.items():
            start = 0
            while True:
                idx = text.find(chunk, start)
                if idx == -1:
                    break
                end = idx + len(chunk)
                key = (idx, end, chunk)
                if key not in seen:
                    seen.add(key)
                    results.append((doc_id, text, idx, end, chunk, label))
                start = idx + 1

        results.sort(key=lambda r: r[2])
        yield from results


def write_csv(rows, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["doc_id", "text", "begin", "end", "chunk", "chunk_label"])
        for row in rows:
            writer.writerow(row)


def summarize(rows, out_path):
    labels, docs = {}, set()
    for r in rows:
        docs.add(r[0])
        labels[r[5]] = labels.get(r[5], 0) + 1
    print(f"  {len(rows)} annotations, {len(docs)} documents -> {out_path}")
    for lbl, cnt in sorted(labels.items(), key=lambda x: -x[1]):
        print(f"    {lbl}: {cnt}")


def verify(rows):
    for doc_id, text, begin, end, chunk, label in rows:
        actual = text[begin:end]
        if actual != chunk:
            print(
                f"OFFSET ERROR doc {doc_id}: expected {chunk!r}, got {actual!r}",
                file=sys.stderr,
            )
            sys.exit(1)
    print("  All offsets verified OK.")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic benchmark CSVs")
    parser.add_argument(
        "--domain",
        choices=["medical", "finance", "all"],
        default="all",
        help="Which domain(s) to generate (default: all)",
    )
    args = parser.parse_args()

    base_dir = os.path.join(os.path.dirname(__file__), "..", "data")

    targets = []
    if args.domain in ("medical", "all"):
        targets.append(("medical", MEDICAL_DOCUMENTS, "synthetic_benchmark_medical.csv"))
    if args.domain in ("finance", "all"):
        targets.append(("finance", FINANCE_DOCUMENTS, "synthetic_benchmark_finance.csv"))

    all_rows = []
    for label, docs, filename in targets:
        rows = list(build_rows(docs))
        out_path = os.path.join(base_dir, filename)
        write_csv(rows, out_path)
        print(f"{label}:")
        summarize(rows, out_path)
        verify(rows)
        all_rows.extend(rows)

    # Also write a combined file when generating both
    if args.domain == "all":
        combined_path = os.path.join(base_dir, "synthetic_benchmark.csv")
        write_csv(all_rows, combined_path)
        print(f"combined:")
        summarize(all_rows, combined_path)
        verify(all_rows)


if __name__ == "__main__":
    main()
