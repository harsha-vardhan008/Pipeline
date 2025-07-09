import re

def parse_single_resume(text):
    lines = text.splitlines()

    # Initialize
    name = "Unknown"
    email = "Not found"
    mobile = "Not found"
    # linkedin = "Not found"
    address = ""
    role = ""

    # Check for combined name and email
    name_email_match = re.search(r'^([\w\s]+)\s+Email\s*[:\-]?\s*([\w\.-]+@[\w\.-]+)', text, re.MULTILINE)
    if name_email_match:
        name = name_email_match.group(1).strip()
        email = name_email_match.group(2).strip()
    else:
        name = lines[0].strip() if lines else "Unknown"
        email_match = re.search(r'[\w\.-]+@[\w\.-]+', text)
        email = email_match.group() if email_match else "Not found"

    # Match mobile number (Indian and international formats)
    phone_match = re.search(r'(\+?\d{1,3}[-\s.]*)?(\(?\d{2,4}\)?[-\s.]*)?\d{3,5}[-\s.]?\d{4}', text)
    mobile = phone_match.group() if phone_match else "Not found"

    # # Match LinkedIn
    # linkedin_match = re.search(r'(https?://)?(www\.)?linkedin\.com/in/[A-Za-z0-9\-_]+', text)
    # linkedin = linkedin_match.group() if linkedin_match else "Not found"

    # Match Address (lines containing Address or Location)
    for line in lines:
        match = re.search(r'(Address|Location)[:\-]?\s*(.*)', line, re.IGNORECASE)
        if match:
            address = match.group(2).strip()
            break

    # Match Role (lines with Role/Position/Applying)
    for line in lines:
        match = re.search(r'(Role|Position|Applying for)[:\-]?\s*(.*)', line, re.IGNORECASE)
        if match:
            role = match.group(2).strip()
            break

    # Section Extraction
    experience, summary, skills, education = [], [], [], []
    capture_exp = capture_summary = capture_skills = capture_education = False

    for line in lines:
        line = line.strip()
        if 'Education' in line:
            capture_education = True
            capture_skills = capture_exp = capture_summary = False
            continue
        elif 'Technical Skills' in line or 'Skills' in line:
            capture_skills = True
            capture_education = capture_exp = capture_summary = False
            continue
        elif 'Professional Experience' in line or 'Experience' in line:
            capture_exp = True
            capture_education = capture_skills = capture_summary = False
            continue
        elif 'Projects' in line:
            capture_summary = True
            capture_exp = capture_education = capture_skills = False
            continue
        elif 'Certifications' in line:
            capture_summary = capture_exp = capture_education = capture_skills = False
            continue

        if capture_exp:
            experience.append(line)
        elif capture_summary:
            summary.append(line)
        elif capture_skills:
            skills.append(line)
        elif capture_education:
            education.append(line)

    return {
        'Name': name,
        'Email': email,
        'Mobile': mobile,
        'Address': address,
        'Role': role,
        'Skills': '\n'.join(skills).strip(),
        'Education': '\n'.join(education).strip(),
        'Experience': '\n'.join(experience).strip(),
        'Summary': '\n'.join(summary).strip()
    }

def transform_data(resume_texts):
    structured_data = [parse_single_resume(item['content']) for item in resume_texts]

    import pandas as pd
    return pd.DataFrame(structured_data)
  