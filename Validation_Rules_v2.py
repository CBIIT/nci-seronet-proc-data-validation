bio_type_list = ["Serum", "Plasma", "EDTA Plasma", "PBMC", "Blood", "Dried Blood Spot", "Saliva",
                 "Nasal swab", "Bronchoalveolar lavage", "Sputum", "Stool", "Urine", "Breast Milk",
                 "Cerebrospinal Fluid", "Rectal Swab", "Vaginal Swab", "Buccal Swab", "Not Reported", "No Specimens Collected"]

state_list = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS',
              'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV',
              'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']


def check_serology_shipping(pd, s3_client, bucket, sql_tuple):
    curr_table = pd.read_sql(("SELECT Barcode_ID, `Material Type` FROM Secondary_Confirm_IDs"), sql_tuple[2])
    key = "Serology_Data_Files/Secondary_Shipping_Manifests/"
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    for curr_file in resp["Contents"]:
        if ("Approved Manifests" in curr_file["Key"]) or ("Failed Manifests" in curr_file["Key"]):
            pass
        elif ".xlsx" in curr_file["Key"]:
            #ship_data = pd_s3.get_df_from_keys(s3_client, bucket, prefix=curr_file["Key"], suffix="xlsx", format="xlsx", na_filter=False, output_type="pandas")
            obj = s3_client.get_object(Bucket=bucket, Key= curr_file["Key"])
            ship_data = pd.read_excel(obj['Body'].read(), engine='openpyxl')
            ship_data.drop(ship_data.columns[10:], axis=1, inplace=True)
            col_list = ship_data.loc[6]
            ship_data.drop(ship_data.index[range(0, 7)], inplace=True)
            ship_data.columns = col_list.tolist()
            z = ship_data.query("Readable_ID not in ['Empty']")
            merge_table = z.merge(curr_table, left_on=["Readable_ID"], right_on=['Barcode_ID'], how="left", indicator=True)
            merge_table = merge_table.query("_merge in ['both']")
            if len(merge_table) > 0:
                print("\n" + curr_file["Key"] + " was found and has Shipping Errors")
            else:
                print("\n" + curr_file["Key"] + " was found and is good to Process")


def Validation_Rules(re, datetime, current_object, data_table, file_name, valid_cbc_ids, drop_list, study_type):
    col_list = current_object.Data_Object_Table[file_name]["Column_List"]
    if len(col_list) > 0:
        data_table.drop_duplicates(col_list, inplace=True)
    data_table.reset_index(inplace=True)
    data_table.drop(columns="index", inplace=True)
    data_table = data_table.apply(lambda x: x.replace('–', '-'))

    min_date = datetime.date(1900,  1,  1)
    max_date = datetime.date.today()
    curr_year = max_date.year
    for header_name in data_table.columns:
        if header_name in drop_list:
            continue
        Rule_Found = False
########################################################################################################################
        if "_sql.csv" in file_name:
            Required_column = "No"
            Rule_Found = True
        else:
            Required_column, Rule_Found = check_ID_validation(header_name, current_object, file_name, data_table, re,
                                                              valid_cbc_ids, Rule_Found)
########################################################################################################################
        if file_name in ["demographic.csv", "baseline.csv"]:
            Required_column, Rule_Found = check_demographic(header_name, current_object, data_table, file_name,
                                                            datetime, curr_year, max_date, Rule_Found, study_type)
        if file_name in ["biospecimen.csv"]:
            Required_column, Rule_Found = check_biospecimen(header_name, current_object, data_table, file_name,
                                                            datetime, max_date, curr_year, Rule_Found)
        if file_name in ["aliquot.csv", "equipment.csv", "reagent.csv", "consumable.csv"]:
            Required_column, Rule_Found = check_processing_rules(header_name, current_object, data_table, file_name,
                                                                 datetime, max_date, Rule_Found)
        if file_name in ["assay.csv", "assay_target.csv", "assay_qc.csv"]:
            Required_column, Rule_Found = check_assay_rules(header_name, current_object, data_table, file_name, Rule_Found)
########################################################################################################################
        if study_type in ["Refrence_Pannel"]:
            if file_name in ["prior_clinical_test.csv"]:
                Required_column, Rule_Found = check_prior_clinical(header_name, current_object, data_table, file_name,
                                                                   datetime, max_date, curr_year, Rule_Found)
            if file_name in ["confirmatory_clinical_test.csv", "serology_confirmation_test_results.csv",
                             "assay_validation.csv", "biospecimen_test_result.csv"]:
                Required_column, Rule_Found = check_confimation_rules(header_name, current_object, data_table, file_name,
                                                                      datetime, min_date, max_date, Rule_Found, re)
            if "SecondaryConfirmationTest" in file_name or "secondary_confirmation_test" in file_name:
                Required_column, Rule_Found = check_confimation_rules(header_name, current_object, data_table, file_name,
                                                                      datetime, min_date, max_date, Rule_Found, re)

        elif study_type in ["Vaccine_Response"]:
            Required_column, Rule_Found = check_all_sheet_rules(header_name, current_object, file_name, data_table, Rule_Found)
            if file_name in ["baseline.csv", "follow_up.csv"]:
                Required_column, Rule_Found = check_base_line_demo(header_name, current_object, data_table, file_name,
                                                                   datetime, curr_year, max_date, Rule_Found)
            if file_name in ["covid_vaccination_status.csv"]:
                Required_column, Rule_Found = check_vaccine_status(header_name, current_object, data_table, file_name, Rule_Found)
            if file_name in ["covid_history.csv"]:
                Required_column, Rule_Found = check_covid_hist(header_name, current_object, data_table, file_name, Rule_Found)
            if file_name in ["treatment_history.csv"]:
                Required_column, Rule_Found = check_treatment_hist(header_name, current_object, data_table,
                                                                   file_name, datetime, curr_year, max_date, Rule_Found)
            if file_name in ["cancer_cohort.csv", "hiv_cohort.csv", "organ_transplant_cohort.csv",
                             "autoimmune_cohort.csv"]:
                check_cohort_data(header_name, current_object, data_table, file_name, Rule_Found)
            if file_name in ["assay_validation.csv", "biospecimen_test_result.csv"]:
                Required_column, Rule_Found = check_confimation_rules(header_name, current_object, data_table, file_name,
                                                                      datetime, min_date, max_date, Rule_Found, re)

        if file_name in ["Validation_Panel.xlsx"]:
            Required_column, Rule_Found = check_confimation_rules(header_name, current_object, data_table, file_name,
                                                                  datetime, min_date, max_date, Rule_Found, re)
        if file_name in ["biorepository_id_map.csv", "reference_panel.csv", "autoimmune_cohort"]:
            Required_column, Rule_Found = check_biorepo_rules(header_name, current_object, data_table, file_name,
                                                              Rule_Found, valid_cbc_ids)
###################################################################################################################
        if (header_name in ['Total_Cells_Hemocytometer_Count', 'Total_Cells_Automated_Count']):
            current_object.compare_total_to_live(file_name, data_table, header_name)
        if (header_name in ['Viability_Hemocytometer_Count',  'Viability_Automated_Count']):
            current_object.compare_viability(file_name, data_table, header_name)
        if header_name in ["Comments"]:   # must be a non-empty string,  N/A is allowed if no comments
            Required_column = "No"
            Rule_Found = True
            current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"])
        if Rule_Found is False:
            pass
            #  print(f"{ file_name}: Column_Name: {header_name } has no validation rules set")
        else:
            current_object.get_missing_values(file_name, data_table, header_name, Required_column)
    return current_object


def compare_tests(current_object):
    file_list = current_object.Part_List      # list of files with Research Part ID
    if ("prior_clinical_test.csv" in file_list) and ("confirmatory_clinical_test.csv" in file_list):
        prior_data = current_object.Data_Object_Table["prior_clinical_test.csv"]["Data_Table"]
        confirm_data = current_object.Data_Object_Table["confirmatory_clinical_test.csv"]["Data_Table"]
        assay_data = current_object.Data_Object_Table["assay.csv"]["Data_Table"]
        assay_target = current_object.Data_Object_Table["assay_target.csv"]["Data_Table"]
        assay_data = assay_data.merge(assay_target)

        merged_data = prior_data.merge(confirm_data, on="Research_Participant_ID", how="outer")
        merged_data = merged_data.merge(assay_data, how="left")
        merged_data = merged_data[["Assay_ID", "Assay_Target", "Assay_Target_Sub_Region",
                                   "Measurand_Antibody", "Research_Participant_ID", "SARS_CoV_2_PCR_Test_Result",
                                   "Assay_Target_Organism", "Interpretation"]]
        part_list = list(set(merged_data["Research_Participant_ID"].tolist()))
        target_virus = ["SARS-CoV-2 Virus", "SARS-COV-2", "SARS-CoV-2"]

        header = "Research_Participant_ID"
        for iterP in part_list:
            curr_part = merged_data.query("Research_Participant_ID == @iterP and Assay_Target_Organism in @target_virus")
            if len(curr_part) == 0:     # SARS_Cov-2 confirm test is missing
                error_msg = "Participant is Missing SARS_Cov-2 Confirmatory Test"
                curr_part = merged_data.query("Research_Participant_ID == @iterP")
                current_object.add_error_values("Error", "confirmatory_clinical_test.csv", -5,
                                                header, curr_part.iloc[0][header], error_msg)
            else:
                test_res, neg_count = get_curr_tests(curr_part, "Negative", target_virus)
                if (len(neg_count) > 0) and (test_res[0] != len(neg_count)):
                    error_msg = ("Participant has a prior test of SARS-Cov2: Negative, but has one or more " +
                                 "SARS_Cov2 Confimatory Tests that are Positive/Reactive or Indetertimate")
                    current_object.add_error_values("Error", "Prior_Vs_Confirm_Test.csv",
                                                    -5, header, curr_part.iloc[0][header], error_msg)
                test_res, pos_count = get_curr_tests(curr_part, "Positive", target_virus)
                if (len(pos_count) > 0) and (test_res[1] == 0):
                    error_msg = ("Participant has a prior test of SARS-Cov2: Positive, " +
                                 "but all SARS_Cov2 Confimatory Tests are Negative/Non-Reactive or Indetertimate")
                    current_object.add_error_values("Error", "Prior_Vs_Confirm_Test.csv", -5, header,
                                                    curr_part.iloc[0][header], error_msg)


def get_curr_tests(curr_part, prior_stat, target_virus):
    part_test = curr_part[curr_part.apply(lambda x: (x['SARS_CoV_2_PCR_Test_Result'] == prior_stat) and
                                          (x["Assay_Target_Organism"] in target_virus), axis=1)]["Interpretation"]
    lower_list = [i.lower() for i in part_test.tolist()]
    neg_count = 0
    pos_count = 0
    inc_count = 0
    for iterZ in lower_list:
        if (("negative" in iterZ) or
           (("no" in iterZ) and ("reaction" in iterZ)) or
           (("non" in iterZ) and ("reactive" in iterZ))):
            neg_count = neg_count + 1
        elif (("positive" in iterZ) or
              (("no" not in iterZ) and ("reaction" in iterZ)) or
              (("non" not in iterZ) and ("reactive" in iterZ))):
            pos_count = pos_count + 1
        else:
            inc_count = inc_count + 1
    part_count = curr_part.query("SARS_CoV_2_PCR_Test_Result == @prior_stat")
    return (neg_count, pos_count, inc_count), part_count


def check_ID_Cross_Sheet(current_object, re, file_sep, study_type):
    if study_type == "Refrence_Pannel":
        current_object.get_cross_sheet_ID(re, 'Research_Participant_ID', file_sep)
    current_object.get_cross_sheet_ID(re, 'Biospecimen_ID', file_sep)
    key_list = ["Assay_ID", "Assay_Target_Organism", "Assay_Target", "Assay_Target_Sub_Region"]
    error_msg = "Assay Values do not match between confirmatory_clinical_test and assay_target"
    current_object.compare_assay_data("confirmatory_clinical_test.csv", "assay_target.csv", key_list, error_msg)


def check_all_sheet_rules(header_name, current_object, file_name, data_table, Rule_Found, Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif "Cohort" in header_name:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
        current_object.check_in_meta(file_name, data_table, header_name, "study_design.csv", "Cohort_Name")
    elif header_name in ["Visit", "Visit_Number"]:
        list_values = ["Baseline(1)"] + list(range(1, 50)) + [z+i for i in ["A", "B", "C" ,"D"] for z in ["0", "1", "2", "3", "4"]]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    else:
        return Required_column, False
    return Required_column, True


def check_ID_validation(header_name, current_object, file_name, data_table, re, valid_cbc_ids,
                        Rule_Found, Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif header_name in ['Research_Participant_ID']:
        pattern_str = '[_]{1}[A-Z, 0-9]{6}$'
        current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "XX_XXXXXX")
        if (file_name in ["demographic.csv", "prior_clinical_test.csv", "baseline.csv"]):
            current_object.check_for_dup_ids(file_name, header_name)
    elif header_name in ["Biospecimen_ID"]:
        pattern_str = '[_]{1}[A-Z, 0-9]{6}[_]{1}[A-Z, 0-9]{3}$'
        current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "XX_XXXXXX_XXX")
        if (header_name in ['Research_Participant_ID']) and (header_name in ["Biospecimen_ID"]):
            current_object.check_if_substr(data_table, "Research_Participant_ID", "Biospecimen_ID", file_name, header_name)
        if file_name in ["biospecimen.csv"]:
            current_object.check_for_dup_ids(file_name, header_name)
    elif header_name in ["Aliquot_ID", "CBC_Biospecimen_Aliquot_ID"]:
        pattern_str = '[_]{1}[A-Z, 0-9]{6}[_]{1}[A-Z, 0-9]{3}[_]{1}[0-9]{1,2}$'
        current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "XX_XXXXXX_XXX_XX")

        if ("Aliquot_ID" in data_table.columns) and ("Biospecimen_ID" in data_table.columns):
            current_object.check_if_substr(data_table, "Biospecimen_ID", "Aliquot_ID", file_name, header_name)
        if ("Aliquot_ID" in data_table.columns):
            current_object.check_for_dup_ids(file_name, header_name)
    elif header_name in ["Assay_ID"]:
        if file_name not in ["Validation_Panel.xlsx"]:
            pattern_str = '[_]{1}[0-9]{3}$'
            current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "XX_XXX")
            current_object.check_assay_special(data_table, header_name, "assay.csv", file_name, re)
        if file_name in ["assay.csv"]:
            current_object.check_for_dup_ids(file_name, header_name)
    elif header_name in ["Biorepository_ID", "Parent_Biorepository__ID"]:
        pattern_str = 'LP[0-9]{5}[ ]{1}0001$'
        current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "LPXXXXXX 0001")
    elif header_name in ["Subaliquot_ID"]:
        if "blinded_validation_panel" in file_name:
            pattern_str = 'LP[0-9]{5}[ ]{1}[9]{1}[0-9]{3}$'
        else:
            pattern_str = 'LP[0-9]{5}[ ]{1}[9]{1}[0-9]{3}$'
            current_object.check_id_field(file_name, data_table, re, header_name, pattern_str, valid_cbc_ids, "LPXXXXXX 9XXX")
        if ("Biorepository_ID" in data_table.columns):
            current_object.check_if_substr_2(data_table, "Biorepository_ID", "Subaliquot_ID", file_name, header_name)
        elif ("Parent_Biorepository_ID" in data_table.columns):
            current_object.check_if_substr_2(data_table, "Parent_Biorepository_ID", "Subaliquot_ID", file_name, header_name)
    elif header_name in "Reporting_Laboratory_ID":
        current_object.check_if_cbc_num(file_name, header_name, data_table, valid_cbc_ids)
    elif header_name in "Visit_Info_ID":
        pass
    else:
        return Required_column, False
    return Required_column, True



def check_prior_clinical(header_name, current_object, data_table, file_name, datetime, max_date, curr_year,
                         Rule_Found, Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif header_name in ['SARS_CoV_2_PCR_Test_Result_Provenance']:
        list_values = ['From Medical Record', 'Self-Reported']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif header_name in ['SARS_CoV_2_PCR_Test_Result']:
        list_values = ['Positive', 'Negative']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif 'Test_Result_Provenance' in header_name:  # checks result proveance for valid input options
        Required_column = "Yes: SARS-Negative"
        list_values = ['Self-Reported', 'From Medical Record', 'N/A']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif ('Test_Result' in header_name) or (header_name in ["Seasonal_Coronavirus_Serology_Result",
                                                            "Seasonal_Coronavirus_Molecular_Result"]):
        Required_column = "Yes: SARS-Negative"
        pos_list = ['Positive', 'Negative', 'Equivocal', 'Not Performed', 'N/A']
        neg_list = ['Positive', 'Negative', 'Equivocal', 'Not Performed']
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Positive"], pos_list)
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Negative"], neg_list)
        current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                               'SARS_CoV_2_PCR_Test_Result', ["Positive", "Negative"])
    elif ('infection_unit' in header_name) or ('HAART_Therapy_unit' in header_name):
        Required_column = "No"
        duration_name = header_name.replace('_unit', '')
        current_object.check_in_list(file_name, data_table, header_name, duration_name, "Is A Number",
                                     ["Day", "Month", "Year"])
        current_object.check_in_list(file_name, data_table, header_name, duration_name, ["N/A"], ["N/A"])
        current_object.unknow_number_dependancy(file_name, header_name, data_table, duration_name, ["N/A"])
    elif ('Duration_of' in header_name) and (('infection' in header_name) or ("HAART_Therapy" in header_name)):
        Required_column = "No"
        if 'HAART_Therapy' in header_name:
            current_name = 'On_HAART_Therapy'
        else:
            current_name = header_name.replace("Duration_of_Current", 'Current')
            current_name = current_name.replace('Duration_of', 'Current')

        current_object.check_in_list(file_name, data_table, header_name, current_name, ['No', 'Unknown', 'N/A'], ["N/A"])
        current_object.check_if_number(file_name, data_table, header_name, current_name, ['Yes'], ["N/A"], 0, 365, "float")
        current_object.unknow_number_dependancy(file_name, header_name, data_table, current_name, ['Yes', 'No', 'Unknown', 'N/A'])
    elif (('Current' in header_name) and ('infection' in header_name)) or (header_name in ["On_HAART_Therapy"]):
        Required_column = "Yes: SARS-Negative"
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Positive"], ['Yes', 'No', 'Unknown', 'N/A'])
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Negative"], ['Yes', 'No', 'Unknown'])
        current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                               'SARS_CoV_2_PCR_Test_Result', ["Positive", "Negative"])
    else:
        Duration_Rules = []
        if ("SARS_CoV_2_PCR" in header_name):
            Duration_Rules = get_duration("SARS_CoV_2_PCR", "Sample_Collection")
        elif ("CMV_Serology" in header_name):
            Duration_Rules = get_duration("CMV", "Serology_Test")
        elif ("CMV_Molecular" in header_name):
            Duration_Rules = get_duration("CMV", "Molecular_Test")
        elif ("EBV_Serology" in header_name):
            Duration_Rules = get_duration("EBV", "Serology_Test")
        elif ("EBV_Molecular" in header_name):
            Duration_Rules = get_duration("EBV", "Molecular_Test")
        elif ("HIV_Serology" in header_name):
            Duration_Rules = get_duration("EBV", "Serology_Test")
        elif ("HIV_Molecular" in header_name):
            Duration_Rules = get_duration("EBV", "Molecular_Test")
        elif ("HepB_Serology" in header_name):
            Duration_Rules = get_duration("HepB", "Serology_Test")
        elif ("HepB_sAg" in header_name):
            Duration_Rules = get_duration("HepB", "sAg")
        elif ("Seasonal_Coronavirus" in header_name):
            Duration_Rules = get_duration("EBV", "Serology_Test")
        elif ("Seasonal_Coronavirus" in header_name):
            Duration_Rules = get_duration("EBV", "Molecular_Test")

        if len(Duration_Rules) > 0:
            current_object.check_duration_rules(file_name, data_table, header_name, "None", "None",
                                                max_date, curr_year, Duration_Rules)
        return Required_column, False
    return Required_column, True


def check_demographic(header_name, current_object, data_table, file_name, datetime, curr_year, max_date,
                      Rule_Found, study_type, Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif (header_name in ['Age']):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", ["Not Reported", "90+"], 0, 150, "float")
    elif (header_name in ['Race', 'Ethnicity', 'Gender', 'Sex_At_Birth']):
        if (header_name in ['Race']):
            list_values = ['White', 'American Indian or Alaska Native', 'Black or African American', 'Asian',
                           'Native Hawaiian or Other Pacific Islander', 'Other', 'Multirace', 'Unknown']  # removing 'Not Reported'
        elif (header_name in ['Ethnicity']):
            list_values = ['Hispanic or Latino', 'Not Hispanic or Latino', 'Unknown',  'Not Reported']
        elif (header_name in ['Gender', 'Sex_At_Birth']):
            list_values = ['Male', 'Female', 'InterSex', 'Not Reported', 'Prefer Not to Answer', 'Unknown', 'Other']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif (header_name in ['Is_Symptomatic']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Positive"], ['Yes', 'No'])
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Negative"], ['No', 'N/A'])
        current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                               "SARS_CoV_2_PCR_Test_Result", ['Positive', 'Negative'])
    elif ("Post_Symptom_Onset" in header_name) or (header_name in ['Symptom_Onset_Year']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_in_list(file_name, data_table, header_name, "Is_Symptomatic", ["No", "N/A"], ["N/A"])
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Is_Symptomatic", ["Yes", "No", "N/A"])
        Duration_Rules = ['Post_Symptom_Onset_Duration', 'Post_Symptom_Onset_Duration_Unit',
                          'Symptom_Onset_Year']
        current_object.check_duration_rules(file_name, data_table, header_name, "Is_Symptomatic", ["Yes"],
                                            max_date, curr_year, Duration_Rules)
    elif ("Post_Symptom_Resolution" in header_name) or (header_name in ['Symptom_Resolution_Year']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_in_list(file_name, data_table, header_name, "Symptoms_Resolved", ["No", "N/A"], ["N/A"])
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Symptoms_Resolved", ["Yes", "No", "N/A"])
        Duration_Rules = ['Post_Symptom_Resolution_Duration', 'Post_Symptom_Resolution_Duration_Unit',
                          'Symptom_Resolution_Year']
        current_object.check_duration_rules(file_name, data_table, header_name, "Symptoms_Resolved", ["Yes"],
                                            max_date, curr_year, Duration_Rules)
    elif (header_name in ['Symptoms_Resolved']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_in_list(file_name, data_table, header_name, "Is_Symptomatic", ["Yes"], ["Yes", "No"])
        current_object.check_in_list(file_name, data_table, header_name, "Is_Symptomatic", ["No", "N/A"], ["N/A"])
        current_object.unknown_list_dependancy(file_name, header_name, data_table, 'Is_Symptomatic', ["Yes", "No", "N/A"])
    elif (header_name in ['Covid_Disease_Severity']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_if_number(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                       ["Positive"], [], 1, 8, "int")
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Negative"], [0])
        current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                               "SARS_CoV_2_PCR_Test_Result", ['Positive', 'Negative'])
    elif (header_name in ["Diabetes_Mellitus", "Hypertension", "Severe_Obesity", "Cardiovascular_Disease",
                          "Chronic_Renal_Disease", "Chronic_Liver_Disease", "Chronic_Lung_Disease",
                          "Immunosuppressive_conditions", "Autoimmune_condition", "Inflammatory_Disease"]):
        if study_type == "Refrence_Pannel":
            Required_column = "Yes: SARS-Positive"
            current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                         ["Positive"], ['Yes', 'No', "Unknown"])
            current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                         ["Negative"], ["Yes", "No", "Unknown"])
            current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                                   "SARS_CoV_2_PCR_Test_Result", ['Positive', 'Negative'])
    elif (header_name in ["Other_Comorbidity"]):
        Required_column = "No"
        current_object.check_icd10(file_name, data_table, header_name)
    else:
        return Required_column, False
    return Required_column, True


def check_base_line_demo(header_name, current_object, data_table, file_name, datetime, curr_year, max_date,
                         Rule_Found, Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif "Visit_Date_Duration_From_Index" in header_name:
        current_object.check_if_number(file_name, data_table, header_name, 'None', "None", ["Unknown"], -1e9, 1e9, "int")
    elif header_name in ["Weight"]:
        current_object.check_if_number(file_name, data_table, header_name, 'None', "None",
                                       ["Not Reported", "N/A"], 1, 1000, "float")  # heaviest weight 1000 lbs (can adjust)
    elif header_name in ["Height"]:
        current_object.check_if_number(file_name, data_table, header_name, 'None', "None",
                                       ["Not Reported", "N/A"], 1, 96, "float")   # 96 inch is 8 feet tall
    elif header_name in ["BMI"]:
        current_object.check_if_number(file_name, data_table, header_name, 'None', "None",
                                       ["Not Reported", "N/A"], 1, 100, "float")
    elif header_name in ["Location", "Biospecimens_Collected", "Biospecimens Collected", "Baseline", "Baseline_Visit"]:
        if header_name in ['Location']:
            list_values = ['Northeast', 'Midwest', 'South', 'West', 'Not Reported', 'Other'] + state_list
        if header_name in ['Biospecimens_Collected', "Biospecimens Collected"]:
            list_values = bio_type_list
        if header_name in ["Baseline", "Baseline_Visit"]:
            list_values = ["Yes", "No"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif header_name in ["Diabetes", "Hypertension", "Obesity", "Cardiovascular_Disease", "Chronic_Lung_Disease",
                         "Chronic_Kidney_Disease", "Chronic_Liver_disease", "Acute_Liver_Disease",
                         "Immunosuppressive_Condition", "Autoimmune_Disorder", "Chronic_Neurological_Condition",
                         "Chronic_Oxygen_Requirement", "Inflammatory_Disease", "Viral_Infection", "Bacterial_Infection",
                         "Cancer", "Substance_Abuse_Disorder", "Organ_Transplant_Recipient"]:
        if file_name in "baseline.csv":
            list_values = ["Yes", "No", "Unknown", "Not Reported"]
        else:
            list_values = ["New Condition", "No Change", "Condition Improved", "Condition Deteriorated",
                           "Condition Resolved", "Condition Status Unknown", "Not Reported", "N/A"]
        if (header_name in "Obesity") and (file_name in "baseline.csv"):
            list_values = ["Underweight", "Normal Weight", "Overweight", "Obesity", "Class 1 Obesity", "Class 2 Obesity",
                           "Class 3 Obesity", "Not Reported"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
#        current_object.check_pike_dups(file_name, header_name)
    elif "Other_Health_Condition_Description_Or_ICD10_codes" in header_name:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"])
        current_object.check_pike_dups(file_name, header_name)
    elif "Obesity_Description_Or_ICD10_codes" in header_name:
        current_object.check_if_string(file_name, data_table, header_name, 'None', "None", ["N/A"], required="optional")
        dep_list = ["New Condition", "No Change", "Condition Improved", "Condition Deteriorated",
                    "Condition Resolved", "Condition Status Unknown", "Not Reported"]
        if (file_name in "baseline.csv"):
            dep_list = ["Underweight", "Normal Weight", "Overweight", "Obesity", "Class 1 Obesity", "Class 2 Obesity",
                        "Class 3 Obesity", "Not Reported"]
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Obesity", dep_list)
        current_object.check_pike_dups(file_name, header_name)
    elif "ICD10_codes" in header_name or "ICD-10_codes" in header_name:
        if "Organ_Transplant" in header_name:
            depend_col = "Organ_Transplant_Recipient"
        elif "_Description_Or_ICD10_codes_or_Type" in header_name:
            depend_col = header_name.replace("_Description_Or_ICD10_codes_or_Type", "")
        elif "_Description_Or_ICD10_codes" in header_name:
            depend_col = header_name.replace("_Description_Or_ICD10_codes", "")
        elif "_ICD10_codes_Or_Agents" in header_name:
            depend_col = header_name.replace("_ICD10_codes_Or_Agents", "")
        else:
            depend_col = header_name.replace("_ICD10_codes", "")
        if file_name in "baseline.csv":
            validate_comorbid_types(current_object, file_name, data_table, header_name, depend_col,
                                    ["No", "Unknown", "Not Reported"], ["Yes"])
        else:
            validate_comorbid_types(current_object, file_name, data_table, header_name, depend_col, ["N/A", "Not Reported"],
                                    ["New Condition", "No Change", "Condition Improved", "Condition Deteriorated",
                                     "Condition Resolved", "Condition Status Unknown", "Not Reported"])
            current_object.compare_list_sizes(file_name, header_name, depend_col)  # follow_up only
    elif header_name in ["ECOG_Status"]:
        list_values = [0, 1, 2, 3, 4, "Not Reported"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif header_name in ["Smoking_Or_Vaping_Status", "Alcohol_Use", "Drug_Use"]:
        if file_name in "follow_up.csv":
            list_values = ["No Change", "Increase in usage frequency", "Decrease in usage frequency",
                           "Stopped usage", "New User", "Usage Status Unknown", "Not Reported"]
        elif "Smoking_Or_Vaping_Status" in header_name:
            list_values = ["Never smoker", "Former smoker", "Current non-smoker", "Current every day smoker",
                           "Current smoker, frequency unknown", "Current some day smoker",
                           "Smoker, current status unknown", "Not Reported"]
        elif "Alcohol_Use" in header_name:
            list_values = ["Never user", "Current non-drinker", "Former drinker", "Current infrequent drinker", "Current light drinker",
                           "Current moderate drinker", "Current heavier drinker", "Current drinker, frequency unknown",
                           "Not Reported"]
        elif "Drug_Use" in header_name:
            list_values = ["Never user", "Current non-user", "Current every day user", "Current some day user", "Former user",
                           "User, current status unknown", "Current user, frequency unknown", "Not Reported"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
        # current_object.check_pike_dups(file_name, header_name)
    elif "Drug_Type" in header_name:
        current_object.check_in_list(file_name, data_table, header_name, "Drug_Use", ["Never user", "Current non-user", "Not Reported"], ["Not Reported", "N/A"])
        current_object.check_if_string(file_name, data_table, header_name, "Drug_Use",
                                       ["Current every day user", "Current some day user", "Former user",
                                        "User, current status unknown", "Current user, frequency unknown"], ["N/A"],
                                       required="Required, if participant is reporting.")
        if file_name in "baseline.csv":
            list_values = ["Never user", "Current non-user", "Current every day user", "Current some day user", "Former user",
                           "User, current status unknown", "Current user, frequency unknown", "Not Reported"]
        elif file_name in "follow_up.csv":
            list_values = ["No Change", "Increase in usage frequency", "Decrease in usage frequency",
                           "Stopped usage", "New user", "Usage Status Unknown", "Not Reported"]

        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Drug_Use", list_values)
        current_object.compare_list_sizes(file_name, "Drug_Type", "Drug_Use")
    elif "Vaccination_Record" in header_name:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"],
                                       required="Required, if CBC has access to data.")
    elif header_name in ["Number_of_Missed_Scheduled_Visits"]:
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", ["N/A"], 0, 100, "int",
                                       required="Optional")
    elif header_name in ["Unscheduled_Visit"]:
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", ["Yes", "No"])
    else:
        return Required_column, False
    return Required_column, True


def validate_comorbid_types(current_object, file_name, data_table, header_name, depend_col, no_values, yes_values):
    current_object.check_in_list(file_name, data_table, header_name, depend_col, ["N/A"], ["N/A"])
    if file_name in "baseline.csv":
        current_object.check_in_list(file_name, data_table, header_name, depend_col, ["Not Reported"], ["Not Reported", "N/A"])
    else:
        current_object.check_in_list(file_name, data_table, header_name, depend_col, ["Not Reported"], ["Not Reported"])
    current_object.check_if_string(file_name, data_table, header_name, depend_col, yes_values, [],
                                   required="Required, if CBC has access to data.")
    current_object.unknown_list_dependancy(file_name, header_name, data_table, depend_col, no_values + yes_values)


def check_biospecimen(header_name, current_object, data_table, file_name, datetime, max_date, curr_year, Rule_Found,
                      Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif(header_name in ["Biospecimen_Group"]):
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Positive"], ['Positive Sample'])
        current_object.check_in_list(file_name, data_table, header_name, 'SARS_CoV_2_PCR_Test_Result',
                                     ["Negative"], ['Negative Sample'])
        current_object.unknown_list_dependancy(file_name, header_name, data_table,
                                               "SARS_CoV_2_PCR_Test_Result", ['Positive', 'Negative'])
    elif(header_name in ["Biospecimen_Type"]):
        list_values = ["Serum",  "EDTA Plasma",  "PBMC",  "Saliva",  "Nasal swab"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif(header_name in ["Initial_Volume_of_Biospecimen (mL)"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", ["N/A"], 0, 1e9, "float")
    elif(header_name in ["Biospecimen_Collection_Year"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", ["N/A"], 1900, curr_year, "int")
    elif (header_name in ['Collection_Tube_Type_Expiration_Date']):
        Required_column = "No"
        current_object.check_date(datetime, file_name, data_table, header_name, "None", "None", True, "Date",
                                  max_date, datetime.date(3000, 1, 1))
    elif (header_name in ['Collection_Tube_Type_Lot_Number']):
        Required_column = "No"
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"])
    elif ('Biospecimen_Processing_Batch_ID' in header_name):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif(header_name in ["Storage_Time_at_2_8_Degrees_Celsius"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", ["N/A"], 0, 1e9, "float")
    elif(header_name in ["Storage_Start_Time_at_2-8_Initials", "Storage_End_Time_at_2-8_Initials"]):
        current_object.check_if_string(file_name, data_table, header_name, "Storage_Time_at_2_8_Degrees_Celsius", "Is A Number", [])
        current_object.check_in_list(file_name, data_table, header_name, "Storage_Time_at_2_8_Degrees_Celsius", ["N/A"], ['N/A'])
        current_object.unknow_number_dependancy(file_name, header_name, data_table, "Storage_Time_at_2_8_Degrees_Celsius", ['N/A'])
    elif ((header_name.find('Company_Clinic') > -1) or (header_name.find('Initials') > -1)
          or (header_name.find('Collection_Tube_Type') > -1)):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])

    elif(header_name.find('Hemocytometer_Count') > -1) or (header_name.find('Automated_Count') > -1):
        current_object.check_if_number(file_name, data_table, header_name, "Biospecimen_Type", ["PBMC"],
                                       ["N/A"], 0, 1e9, "float")
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Biospecimen_Type", bio_type_list)
    elif(header_name in ["Centrifugation_Time (min)", "RT_Serum_Clotting_Time (min)"]):
        current_object.check_if_number(file_name, data_table, header_name, "Biospecimen_Type", ["Serum"],
                                       ["N/A"], 0, 1e9, "float")
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Biospecimen_Type", bio_type_list)
    elif ("Duration_Units" in header_name):
        Required_column = "Yes"
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", ['Minute', 'Hour', 'Day'])
    elif ("Biospecimen_Receipt_to_Storage_Duration" in header_name):
        Required_column = "Yes"
        current_object.check_if_number(file_name, data_table, header_name, "Biospecimen_Type", ["Serum"], [], 0, 1e9, "float")
        current_object.check_if_number(file_name, data_table, header_name, "Biospecimen_Type", ["PBMC"], [], 0, 8, "float")
    elif ("Duration" in header_name):
        Required_column = "Yes"
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", [], -1e9, 1e9, "float")
    else:
        return Required_column, False
    return Required_column, True


def check_processing_rules(header_name, current_object, data_table, file_name, datetime, max_date,
                           Rule_Found, Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif (header_name in ["Aliquot_Volume"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", ["N/A"], 0, 1e9, "float")
    elif (header_name in ["Aliquot_Concentration", "Aliquot_Concentration (cells/mL)"]):
        current_object.check_if_number(file_name, data_table, header_name, "Biospecimen_Type", ["PBMC"],
                                       ["N/A"], 0, 1e9, "float")
        current_object.check_in_list(file_name, data_table, header_name, "Biospecimen_Type",
                                     ["Serum", "EDTA Plasma", "Saliva", "Nasal swab"], ["N/A"])
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Biospecimen_Type", bio_type_list)
    elif ('Expiration_Date' in header_name) or ('Calibration_Due_Date' in header_name):
        Required_column = "No"
        current_object.check_date(datetime, file_name, data_table, header_name, "None", "None", True, "Date",
                                  max_date, datetime.date(3000, 1, 1))
    elif ('Lot_Number' in header_name) or ('Catalog_Number' in header_name):
        Required_column = "No"
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"])
    elif (header_name in ["Equipment_Type", "Reagent_Name", "Consumable_Name"]):
        if (header_name in ["Equipment_Type"]):
            list_values = ['Refrigerator', '-80 Refrigerator', 'LN Refrigerator', 'Microsope', 'Pipettor',
                           'Controlled-Rate Freezer', 'Automated-Cell Counter', '-80 Freezer',
                           'LN Freezer', 'Centrifuge', 'Microscope', 'BSC', '4C Refrigerator']

        elif (header_name in ["Reagent_Name"]):
            list_values = (['DPBS', 'Ficoll-Hypaque', 'RPMI-1640, no L-Glutamine', 'Fetal Bovine Serum',
                            '200 mM L-Glutamine', '1M Hepes', 'Penicillin/Streptomycin',
                            'DMSO, Cell Culture Grade', 'Vital Stain Dye'])
        elif (header_name in ["Consumable_Name"]):
            list_values = ["50 mL Polypropylene Tube", "15 mL Conical Tube", "Cryovial Label",
                           "2 mL Cryovial", "2mL Cryovial", "CPT Tube", "SST Tube"]
        current_object.check_in_list(file_name, data_table, header_name, "Biospecimen_Type", ["PBMC"], list_values)
        current_object.unknown_list_dependancy(file_name, header_name, data_table, "Biospecimen_Type", bio_type_list)
    elif ("Aliquot" in header_name) or ("Equipment_ID" in header_name):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    else:
        return Required_column, False
    return Required_column, True


def check_confimation_rules(header_name, current_object, data_table, file_name, datetime, min_date, max_date,
                            Rule_Found, re, Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif header_name in ["Assay_Target"]:
        current_object.check_assay_special(data_table, header_name, "assay_target.csv", file_name, re)
    elif (header_name in ["Instrument_ID", "Test_Operator_Initials", "Assay_Kit_Lot_Number"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif ('Test_Batch_ID' in header_name):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif ('Date_of_Test' in header_name):
        current_object.check_date(datetime, file_name, data_table, header_name, "None", "None", False, "Time")
    elif ("Assay_Target_Organism" in header_name):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
        current_object.check_assay_special(data_table, header_name, "assay.csv", file_name, re)
    elif ("Assay_Cutoff_Units" in header_name):
        current_object.check_in_list(file_name, data_table, header_name, "Assay_Cutoff", ["N/A"], ["N/A"])
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"])
    elif ("Assay_Cutoff" in header_name):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"])
    elif (header_name in ["Assay_Target_Sub_Region", "Measurand_Antibody", "Derived_Result"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"])
    elif (header_name in ["Interpretation"]):
        list_values = ['positive', 'negative', 'reactive', 'reaction', 'equivocal', 'indeterminate']
        current_object.check_interpertation(file_name, data_table, header_name, list_values)
    elif (header_name in ["Duration_Of_Test_Units", "Duration_In_Fridge_Units"]):
        list_values = ['Seconds', 'seconds', 'Minutes', 'minute', 'Hours', 'hour', 'N/A']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif (header_name in ["Duration_Of_Test", "Duration_In_Fridge"]):
        Required_column = "Yes"
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", ["N/A"], 0, 5000, "float")
    elif (header_name in ["Assay_Replicate", "Sample_Dilution"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", [], 0, 200, "float")
    elif (header_name in ["Raw_Result", "Positive_Control_Reading", "Negative_Control_Reading"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", ["N/A"], 0, 1e9, "float")
    elif header_name in ["Sample_Type"]:
        list_values = ['Serum', 'Plasma', 'Venous Whole Blood', 'Dried Blood Spot', 'Nasal Swab',
                       'Broncheolar Lavage', 'Sputum']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif header_name in ["Derived_Result_Units"]:
        current_object.check_if_string(file_name, data_table, header_name, "Derived_Result", "Is A Number", [])
        current_object.check_in_list(file_name, data_table, header_name, "Derived_Result", ["N/A"], ["N/A"])
    elif header_name in ["Raw_Result_Units"]:
        current_object.check_if_string(file_name, data_table, header_name, "Raw_Result", "Is A Number", [])
        current_object.check_in_list(file_name, data_table, header_name, "Raw_Result", ["N/A"], ["N/A"])
    elif ("Biospecimen_Collection_to_Test_Duration" in header_name):
        Required_column = "Yes"
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", [], -1000, 1000, "float")
    else:
        return Required_column, False
    return Required_column, True


def check_assay_rules(header_name, current_object, data_table, file_name, Rule_Found, Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif (header_name in ["Technology_Type", "Assay_Name", "Assay_Manufacturer", "Assay_Target_Organism"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif "Quality_Control" in header_name:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif (header_name in ["EUA_Status", "Assay_Multiplicity", "Assay_Control_Type", "Measurand_Antibody_Type",
                          "Assay_Result_Type", "Peformance_Statistics_Source", "Assay_Antigen_Source"]):
        if (header_name in ["EUA_Status"]):
            list_values = ['Approved', 'Submitted', 'Not Submitted', 'N/A']
        if (header_name in ["Assay_Multiplicity"]):
            list_values = ['Multiplex', 'Singleplex']
        if (header_name in ["Assay_Control_Type"]):
            list_values = ['Internal', 'External', 'Internal and External', 'N/A']
        if (header_name in ["Measurand_Antibody_Type"]):
            list_values = ['IgG', 'IgM', 'IgA', 'IgG + IgM', 'Total', 'N/A']
        if (header_name in ["Assay_Result_Type"]):
            list_values = ['Qualitative', 'Quantitative', 'Semi-Quantitative']
        if (header_name in ["Peformance_Statistics_Source"]):
            list_values = ['Manufacturer',  'In-house']
        if (header_name in ["Assay_Antigen_Source"]):
            list_values = ['Manufacturer',  'In-house', 'N/A']
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif ("Target_biospecimen_is_" in header_name):
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", ["T", "F"])
    elif (header_name in ["Postive_Control", "Negative_Control", "Calibration_Type", "Calibrator_High_or_Positive",
                          "Calibrator_Low_or_Negative"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"])
    elif (header_name in ["Assay_Result_Unit",  "Cut_Off_Unit",  "Assay_Target"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif (header_name in ["Positive_Cut_Off_Threshold",  "Negative_Cut_Off_Ceiling",  "Assay_Target_Sub_Region"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"])
    elif (header_name in ["N_true_positive",  "N_true_negative",  "N_false_positive",  "N_false_negative"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", [], 0, 1e9, "int")
    else:
        return Required_column, False
    return Required_column, True


def check_treatment_hist(header_name, current_object, data_table, file_name, datetime, curr_year, max_date,
                         Rule_Found, Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif "Health_Condition_Or_Disease" in header_name:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif "Treatment" in header_name:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif (header_name in ["Dosage"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", ["Unknown"], 0, 1e9, "float")
    elif (header_name in ["Dosage_Units", "Dosage_Regimen"]):
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif (header_name in ["Start_Date_Duration_From_Index", "Stop_Date_Duration_From_Index"]):
        current_object.check_if_number(file_name, data_table, header_name, "None", "None",
                                       ["Ongoing", "Not Reported"], -1e9, 1e9, "int")
    elif (header_name in ["Update"]):
        list_values = ["Baseline Information", "No Change From Last Visit", "Change in Dosage and/or Regimen",
                       "New Medication", "Stopped", "Not Reported"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    else:
        return Required_column, False
    return Required_column, True


def check_biorepo_rules(header_name, current_object, data_table, file_name, Rule_Found, cbc_list, Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif header_name in ["Destination"]:   # string
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif header_name in ["Reserved", "Used", "Consented_For_Research_Use"]:
        if header_name in ["Reserved", "Used"]:
            list_values = ["Yes", "No"]
        elif header_name in ["Consented_For_Research_Use"]:
            list_values = ["Yes", "No", "Withdrawn"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif header_name in ["Reference_Panel_ID", "Batch_ID"]:
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", False, 0, 1e9, "int")
    elif header_name in ["Panel_Type"]:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif header_name in ["Destination_ID"]:
        current_object.check_if_cbc_num(file_name, header_name, data_table, cbc_list)
    else:
        return Required_column, False
    return Required_column, True


def get_duration(duration_col, col_string):
    dur_list = [f'Post_{duration_col}_{col_string}_Duration',
                f'Post_{duration_col}_{col_string}_Duration_Unit',
                f'{duration_col}_{col_string}_Year']
    return dur_list


def compare_SARS_tests(current_object, pd, conn):
    file_list = current_object.Data_Object_Table
    if ("prior_clinical_test.csv" in file_list) and ("confirmatory_clinical_test.csv" in file_list):
        prior_data = current_object.Data_Object_Table["prior_clinical_test.csv"]["Data_Table"]
        confirm_data = current_object.Data_Object_Table["confirmatory_clinical_test.csv"]["Data_Table"]
        if "Interpretation" not in confirm_data.columns:
            return
        conversion = pd.read_sql(("Select Assay_ID, Target_Organism_Conversion FROM Assay_Organism_Conversion"), conn)
        confirm_data = confirm_data.merge(conversion, how="left")
        confirm_data = confirm_data.query("Target_Organism_Conversion == 'SARS-CoV-2 Virus'")
        confirm_table = pd.crosstab(confirm_data["Research_Participant_ID"], confirm_data["Interpretation"])
        if "Positive" not in confirm_table.columns:
            confirm_table["Positive"] = 0
        if "Negative" not in confirm_table.columns:
            confirm_table["Negative"] = 0
        confirm_table.reset_index(inplace=True)
        merged_data = prior_data[["Research_Participant_ID", "SARS_CoV_2_PCR_Test_Result"]].merge(confirm_table,
                                                                                                  how="left", indicator=True)

        missing_pcr = merged_data.query("_merge not in ['both']")
        pos_pcr_error = merged_data.query("SARS_CoV_2_PCR_Test_Result == 'Positive' and Positive == 0")
        neg_pcr_error = merged_data.query("SARS_CoV_2_PCR_Test_Result == 'Negative' and Positive > 0")

        error_msg = "Participant is missing SARS-CoV-2 Confrimation Testing"
        current_object.update_error_table("Error", missing_pcr, "confirmatory_clinical_test.csv",
                                          "Research_Participant_ID", error_msg)

        error_msg = "Participant is Positive PCR but has Negative SARS-CoV-2 Confrimation Testing"
        current_object.update_error_table("Error", pos_pcr_error, "confirmatory_clinical_test.csv",
                                          "Research_Participant_ID", error_msg)

        error_msg = "Participant is Negative PCR but has Positive SARS-CoV-2 Confrimation Testing"
        current_object.update_error_table("Error", neg_pcr_error, "confirmatory_clinical_test.csv",
                                          "Research_Participant_ID", error_msg)


def check_shipping(current_object, pd, conn):
    file_list = current_object.Data_Object_Table
    aliquot_table = pd.read_sql(("SELECT Aliquot_ID, Aliquot_Volume FROM `seronetdb-Vaccine_Response`.Aliquot"), conn)

    if ("aliquot.csv" in file_list):
        aliquot_df = current_object.Data_Object_Table["aliquot.csv"]["Data_Table"][["Aliquot_ID", "Aliquot_Volume"]]
        aliquot_table = pd.concat([aliquot_table, aliquot_df])
        aliquot_table = aliquot_table.drop_duplicates()

    if ("shipping_manifest.csv" in file_list):
        shipping_table = current_object.Data_Object_Table["shipping_manifest.csv"]["Data_Table"]
        shipping_table["Volume"] = [i/1000 if i >= 1000 else i for i in shipping_table["Volume"].tolist()]
        compare_tables = shipping_table.merge(aliquot_table, left_on=["Current Label"], right_on=["Aliquot_ID"], indicator=True, how="outer")

        match_ids = compare_tables.query("_merge  in ['both']")
        z = match_ids.query("Volume !=  Aliquot_Volume")

        compare_tables = compare_tables.query("_merge in ['left_only']")
        error_msg = "Aliquot ID in Shipping Manifest but not in Aliquot"
        current_object.update_error_table("Error", compare_tables, "shipping_manifest.csv", "Current Label", error_msg)

        error_msg = "Aliquot ID in Shipping Manifest but Volumes are different"
        current_object.update_error_table("Error", z, "shipping_manifest.csv", "Current Label", error_msg)





def check_vaccine_status(header_name, current_object, data_table, file_name, Rule_Found, Required_column="Yes"):
    #has_vaccine = ["Dose 1 of 1", "Dose 1 of 2", "Dose 2 of 2", "Dose 3", "Dose 4"] + ["Booster " + str(i) for i in range(1, 7)]
    no_vaccine = ["No vaccination event reported", "Unvaccinated"]

    has_vaccine = ['Dose 1 of 1', 'Dose 1 of 2', 'Dose 2 of 2', 'Dose 2', 'Dose 3', 'Dose 3:Bivalent', 'Dose 4', 'Dose 4:Bivalent']
    has_vaccine =  has_vaccine + ["Booster " + str(i) for i in list(range(1,10))]
    has_vaccine =  has_vaccine + ["Booster " + str(i) + ":Bivalent" for i in list(range(1,10))]

    if Rule_Found is True:
        pass
    elif header_name in ["Vaccination_Status"]:
        list_values = has_vaccine + no_vaccine
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif header_name in ["Other_SARS-CoV-2_Vaccination_Side_Effects"]:
        current_object.check_if_string(file_name, data_table, header_name, "SARS-CoV-2_Vaccination_Side_Effects", ["Other"], [])
        current_object.check_in_list(file_name, data_table, header_name, "SARS-CoV-2_Vaccination_Side_Effects", "Not Other", ["N/A"])
    elif "SARS-CoV-2_Vaccination_Side_Effects" in header_name:
        list_values = ["Fever", "Fatigue", "Chills", "Headache", "Arm Pain", "Rash", "Muscle Aches",
                       "Anaphylaxis", "Joint Aches", "Other", "No Side Effects Reported", "N/A"]
        current_object.check_in_list(file_name, data_table, header_name, "Vaccination_Status", no_vaccine, ["N/A"])
        current_object.check_in_list(file_name, data_table, header_name, "Vaccination_Status", has_vaccine, list_values)
    elif header_name in ["SARS-CoV-2_Vaccine_Type"]:
        current_object.check_in_list(file_name, data_table, header_name, "Vaccination_Status", no_vaccine, ["N/A"])
        current_object.check_in_list(file_name, data_table, header_name, "Vaccination_Status", ["Dose 1 of 1"], ["Johnson & Johnson"])
        current_object.check_if_string(file_name, data_table, header_name, "Vaccination_Status", has_vaccine, [])
    elif "SARS-CoV-2_Vaccination_Date_Duration_From_Index" in header_name:
        current_object.check_in_list(file_name, data_table, header_name, "Vaccination_Status", no_vaccine, ["N/A"])
        current_object.check_if_number(file_name, data_table, header_name, "Vaccination_Status", has_vaccine, [], -1e9, 1e9, "int")
    else:
        return Required_column, False
    return Required_column, True





def check_covid_hist(header_name, current_object, data_table, file_name, Rule_Found, Required_column="Yes"):
    list_values = ["Positive by PCR", "Negative by PCR", "Positive by Rapid Antigen Test",
                   "Negative by Rapid Antigen Test", "Positive by Antibody Test", "Negative by Antibody Test",
                   "Likely COVID Positive", "No COVID event reported", "No COVID data collected",
                   "Negative, Test Not Specified", "Positive, Test Not Specified"]
    pos_tests = [i for i in list_values if "Positive" in i]
    neg_tests = [i for i in list_values if "Positive" not in i]
    no_tests = ["Likely COVID Positive", "No COVID event reported", "No COVID data collected"]

    pos_data = ["positive" in i.lower() for i in data_table["COVID_Status"].tolist()]
    neg_data = ["positive" not in i.lower() for i in data_table["COVID_Status"].tolist()]

    if Rule_Found is True:
        pass
    elif header_name in ["COVID_Status", "Breakthrough COVID", "Symptomatic_COVID", "Recovered_From_COVID"]:
        if "COVID_Status" in header_name:
            current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
            #  current_object.check_list_errors(file_name, data_table, header_name, list_values, no_tests)
        elif header_name in ["Breakthrough COVID", "Symptomatic_COVID", "Recovered_From_COVID"]:
            current_object.check_in_list(file_name, data_table[neg_data], header_name, "None", "Negative/ Not Reported", ["N/A"])
            current_object.check_in_list(file_name, data_table[pos_data], header_name, "None", "Positive", ["Yes", "No", "Unknown"])
    elif "SARS-CoV-2 Variant" in header_name:
        current_object.check_in_list(file_name, data_table[neg_data], header_name, "None", "Negative/ Not Reported", ["N/A"])
        current_object.check_if_string(file_name, data_table[pos_data], header_name, "None", "Positive", [])
    elif header_name in ["PCR_Test_Date_Duration_From_Index"]:
        current_object.get_test_dur(file_name, data_table, header_name, list_values, "by PCR")
    elif header_name in ["Rapid_Antigen_Test_Date_Duration_From_Index"]:
        current_object.get_test_dur(file_name, data_table, header_name, list_values, "Rapid Antigen Test")
    elif header_name in ["Antibody_Test_Date_Duration_From_Index"]:
        current_object.get_test_dur(file_name, data_table, header_name, list_values, "Antibody Test")
    elif "From_Index" in header_name or "Duration_of_Disease" in header_name:
        current_object.check_in_list(file_name, data_table[neg_data], header_name, "None", "Negative/ Not Reported", ["N/A"])
        current_object.check_if_number(file_name, data_table[pos_data], header_name, "None", "Positive", ["N/A", "Unknown", "Not Reported"],
                                       -1e9, 1e9, "int")
    elif header_name in ["Symptoms"]:
        list_values = ["Runny Nose", "Sore Throat", "Fever", "Chills", "Muscle Ache", "Runny Nose", "Sore Throat",
                       "Loss of Smell", "Loss of Taste", "Cough", "Headache", "Difficulty Breathing",
                       "Nausea or Vomiting", "Diarrhea", "Chest Pain", "Shortness of Breath", "Abdominal Pain", "Rigors",
                       "Wheezing", "Fatigue", "No symptoms reported", "Other", "N/A", "Congestion"]
        current_object.check_in_list(file_name, data_table[neg_data], header_name, "None", "Negative/ Not Reported", ["N/A"])
        current_object.check_in_list(file_name, data_table[pos_data], header_name, "None", "Positive", list_values)

    elif header_name in ["Level_Of_Care", "COVID_complications", "Long_COVID_symptoms"]:
        if header_name in ["Level_Of_Care"]:
            list_values = ["Quarantine at Home", "Outpatient", "Hospitalized", "ICU", "Not reported", "N/A"]
        elif header_name in ["COVID_complications"]:
            list_values = ["Acute Respiratory Failure", "Pneumonia", "Acute Respiratory Distress Syndrome (ARDS)",
                           "Acute Liver injury", "Acute Cardiac Injury (Heart injury)", "Secondary Infection",
                           "Acute Kidney Injury", "Septic Shock", "Disseminated Intravascular Coagulation (DIC)",
                           "Blood Clots", "Chronic Fatigue", "Rhabdomyolysis", "Multisystem Inflammatory Syndrome in Children",
                           "No complications reported", "N/A"]
        elif header_name in ["Long_COVID_symptoms"]:
            list_values = ["Cough", "Chest Pain", "Difficulty walking distances", "Muscle weakness", "Mental confusion",
                           "Numbness or Tingling", "Palpitations", "Shortness of breath (more than before illness)",
                           "Spike in blood pressure", "Supplemental oxygen", "Tired (more than before the illness)",
                           "Other", "No symptoms reported", "N/A"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif "Disease_Severity" in header_name:
        list_values = [0, 1, 2, 3, 4, 5, 6, 7, 8, "Not Reported"]
    elif header_name in ["Other_Symptoms"]:
        current_object.check_if_string(file_name, data_table, header_name, "Symptoms", ["Other"], [])
        current_object.check_in_list(file_name, data_table, header_name, "Symptoms", ["Not Other"], ["N/A"])
    elif header_name in ["Other_Long_COVID_symptoms"]:
        current_object.check_if_string(file_name, data_table, header_name, "Long_COVID_symptoms", ["Other"], [])
        current_object.check_in_list(file_name, data_table, header_name, "Long_COVID_symptoms", ["Not Other"], ["N/A"])
    elif header_name in ["COVID_Therapy"]:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"])
    else:
        return Required_column, False
    return Required_column, True


def check_cohort_data(header_name, current_object, data_table, file_name, Rule_Found, Required_column="Yes"):
    if Rule_Found is True:
        pass
    elif header_name in ["Viral_RNA_Load"]:
        current_object.check_if_number(file_name, data_table, header_name, "None", "None",
                                       ["Not Reported", "Not Detected", "Detected, not Quantifiable"], 0, 1e9, "float")

    elif "Duration_From_Index" in header_name:
        if "Date_of_Latest_Hematopoietic_Cell_Transplant" in header_name:
            depend_col = "Number_of_Hematopoietic_Cell_Transplants"
        elif "Date_of_Latest_Solid_Organ_Transplant" in header_name:
            depend_col = "Number_Of_Solid_Organ_Transplants"
        else:
            depend_col = "None"
        if depend_col == "None":
            current_object.check_if_number(file_name, data_table, header_name, "None", "None",
                                           ["Not Reported"], -1e9, 1e9, "int")
        else:
            current_object.check_in_list(file_name, data_table, header_name, depend_col, [0, "0"], ["N/A"])
            current_object.check_if_number(file_name, data_table, header_name, depend_col, "> 0",
                                           ["Not Reported"], -1e90, 1e90, "int")
    elif header_name in ["CD4_Count"]:
        current_object.check_if_number(file_name, data_table, header_name, "None", "None",
                                       ["Not Reported", "Not Detected", "Detected, not Quantifiable"], 0, 1e9, "float")
    elif header_name in ["Opportunistic_Infection_History", "Cancer", "Autoimmune_Condition"]:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", [])
    elif header_name in ["Antibody_Name"]:
        current_object.check_if_string(file_name, data_table, header_name, "None", "None", ["N/A"])
    elif header_name in ["Antibody_Present"]:
        list_values = ["Yes", "No", "Unknown", "Not Reported", "N/A"]
        current_object.compare_list_sizes(file_name, "Antibody_Name", "Antibody_Present")
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif "ICD_10_Code" in header_name:
        current_object.check_icd10(file_name, data_table, header_name)
    elif header_name in ["Cured", "In_Remission", "In_Unspecified_Therapy", "Chemotherapy", "Radiation Therapy", "Surgery"]:
        if header_name in ["Cured", "In_Remission"]:
            list_values = ["Yes", "No", "Unknown", "Not Reported", "N/A"]
        elif header_name in ["In_Unspecified_Therapy", "Chemotherapy", "Radiation Therapy", "Surgery"]:
            list_values = ["Yes", "No", "Not Reported", "N/A"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif header_name in ["Organ Transplant"]:
        list_values = ["Lung", "Heart", "Kidney", "Liver", "Pancreas", "Hematopoietic Cell Transplant", "Other"]
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)
    elif header_name in ["Organ_Transplant_Other"]:
        current_object.check_if_string(file_name, data_table, header_name, "Organ Transplant", ["Other"], [])
        current_object.check_in_list(file_name, data_table, header_name, "Organ Transplant", "Not Other", ["N/A"])
    elif header_name in ["Number_of_Hematopoietic_Cell_Transplants", "Number_Of_Solid_Organ_Transplants"]:
        current_object.check_if_number(file_name, data_table, header_name, "None", "None", [], 0, 1e9, "int")
    elif header_name in ["Update"]:
        if file_name == "hiv_cohort.csv":
            list_values = ["Baseline information", "Condition Improved", "Condition Worsened", "No Change in Condition",
                           "Change in CD4 count", "Change in Treatment", "Change in Reported Viral RNA Load",
                           "Change in Opportunistic Infection Status", "No Update Reported"]
        elif file_name == "cancer_cohort.csv":
            list_values = ["Baseline information", "Condition Improved", "Condition Worsened", "No Change in Condition",
                           "In Remission", "Change in Treatment", "New Condition", "No Update Reported"]
        elif file_name == "autoimmune_cohort.csv":
            list_values = ["Baseline information", "Condition Improved", "Condition Worsened", "No Change in Condition",
                           "Condition Resolved", "Change in Treatment", "Change in Antibody Levels Reported", "New Condition", "No Update Reported"]
        elif file_name == "organ_transplant_cohort.csv":
            list_values = ["Baseline information", "New Solid Organ Transplant", "New Hematopoietic_Cell_Transplant", "No Changes Reported",
                           "New Transplant Related Treatment", "Transplant Related Treatment Stopped", "Changes to Transplant Related Treatment"]
        else:
            print("issue")
            list_values = []
        current_object.check_in_list(file_name, data_table, header_name, "None", "None", list_values)


def check_comorbid_hist(pd, sql_tuple, curr_obj):
    data_table = curr_obj.Data_Object_Table
    if "baseline.csv" not in data_table or "follow_up.csv" not in data_table:
        return

    list_of_comorbids = ["Diabetes", "Hypertension", "Cardiovascular_Disease", "Chronic_Lung_Disease",
                         "Chronic_Kidney_Disease", "Chronic_Liver_Disease", "Acute_Liver_Disease",
                         "Immunosuppressive_Condition", "Autoimmune_Disorder", "Chronic_Neurological_Condition",
                         "Chronic_Oxygen_Requirement", "Inflammatory_Disease", "Viral_Infection", "Bacterial_Infection",
                         "Cancer", "Substance_Abuse_Disorder", "Organ_Transplant_Recipient"]

    if "baseline.csv" in data_table:
        base_table = data_table["baseline.csv"]["Data_Table"]
        base_table.rename(columns={'Diabetes_Description_Or_ICD10_codes_or_Type': "Diabetes_Description_Or_ICD10_codes"}, inplace=True)
        base_table.reset_index(inplace=True)
    if "follow_up.csv" in data_table:
        followup_table = data_table["follow_up.csv"]["Data_Table"]
        followup_table.reset_index(inplace=True)

    if "Diabetes" not in base_table.columns:
        return

    visit_table = pd.concat([base_table, followup_table])
    visit_table = visit_table.sort_values(["Research_Participant_ID", "Visit_Number"], ascending=(True, True))
    uni_part = list(set(visit_table["Research_Participant_ID"].tolist()))

    for iterZ in list_of_comorbids:
        try:
            if iterZ in ["Organ_Transplant_Recipient"]:
                cat_var = "Organ_Transplant_Recipient"
                type_var = "Organ_Transplant_Description_Or_ICD10_codes"
            else:
                cat_var, type_var = [i for i in visit_table.columns if iterZ in i]
        except Exception as e:
            print(e)


        col_list = ["Research_Participant_ID", cat_var, type_var, "Visit_Number", "Visit_Type"]
        test_table = visit_table[col_list]
        tables_to_check = pd.DataFrame(columns=col_list)
        for curr_part in uni_part:
            filt_table = test_table.query("Research_Participant_ID == @curr_part")
            filt_table.reset_index(inplace=True)
            filt_table.fillna("Data Error", inplace=True)
            filt_table[cat_var] = [i.split("|") for i in filt_table[cat_var]]
            try:
                filt_table[type_var] = [i.split("|") for i in filt_table[type_var]]
            except Exception as e:
                print(e)

            Master_Dict = {}
            for curr_idx in filt_table.index:
                Master_Dict = make_dict(curr_obj, Master_Dict, filt_table,  type_var, cat_var, curr_idx)


def get_visit_list(visit_list, template, data_table):
    if template in data_table:
        new_visits = data_table[template]["Data_Table"]
        new_visits = new_visits["Visit_Info_ID"].tolist()
        visit_list = visit_list + new_visits
    return visit_list


def check_vacc_hist(pd, sql_tuple, curr_obj):
    if len(curr_obj.All_Part_ids) == 0:     # no Participant data in submission, no need to run this function
        return
    data_table = curr_obj.Data_Object_Table

    visit_list = pd.read_sql(("SELECT Visit_Info_ID FROM Participant_Visit_Info;"), sql_tuple[2])
    visit_list = visit_list["Visit_Info_ID"].tolist()

    visit_list = get_visit_list(visit_list, "baseline.csv", data_table)  # new visits from baseline
    visit_list = get_visit_list(visit_list, "followup.csv", data_table)  # new visits from followup
    x = list(set(visit_list))
    all_list = pd.DataFrame({'Visit_Info_ID': x})
    all_list['Research_Participant_ID'] = [i[:9] for i in all_list["Visit_Info_ID"]]
    try:
        filt_visit = curr_obj.All_Part_ids.merge(all_list)
    except Exception as e:
        print(e)

    if "covid_history.csv" in curr_obj.rec_file_names:
        x = data_table["covid_history.csv"]["Data_Table"].merge(filt_visit, how="right", on="Visit_Info_ID", indicator=True)
        error_data = x.query("_merge not in ['both']")
        covid_db = pd.read_sql(("SELECT * FROM Covid_History"), sql_tuple[2])
        error_data = error_data.merge(covid_db["Visit_Info_ID"], how="left", indicator="Check_DB")
        error_data = error_data.query("Check_DB not in ['both']")

        error_msg = "Visit was found in baseline or followup, but there is no event information in covid_history"
        for index in error_data.index:
            curr_obj.add_error_values("Error", "Missing_Visit_Info.csv", int(index) + 1,
                                      "Visit_Info_ID", error_data["Visit_Info_ID"][index], error_msg)

    if "covid_vaccination_status.csv" in curr_obj.rec_file_names:
        vacc_table = data_table["covid_vaccination_status.csv"]["Data_Table"]
        if "SARS-CoV-2_Vaccination_Date_Duration_From_Index" not in vacc_table.columns:  # file not included in submission
            return
        x = vacc_table.merge(filt_visit, how="right", on="Visit_Info_ID", indicator=True)
        error_data = x.query("_merge not in ['both']")
        error_msg = "Visit was found in baseline or followup, but there is no event information in covid_vaccination_status"
        covid_db = pd.read_sql(("SELECT * FROM Covid_Vaccination_Status"), sql_tuple[1])
        error_data = error_data.merge(covid_db["Visit_Info_ID"], how="left", indicator="Check_DB")
        error_data = error_data.query("Check_DB not in ['both']")

        for index in error_data.index:
            curr_obj.add_error_values("Error", "Missing_Visit_Info.csv", int(index) + 1,
                                      "Visit_Info_ID", error_data["Visit_Info_ID"][index], error_msg)

        for curr_id in curr_obj.All_Part_ids["Research_Participant_ID"]:
            curr_part = vacc_table.query("Research_Participant_ID in @curr_id")
            if "Visit_Number" in curr_part.columns:
                curr_part["Visit_Number"] = curr_part["Visit_Number"].replace("Baseline(1)", "1")
                curr_part["Visit_Number"] = curr_part["Visit_Number"].replace("Baseline (1)", "1")
            else:
                curr_part["Visit_Number"] = [float(i[-2:]) for i in curr_part["Visit_Info_ID"]]
            try:
                curr_part["SARS-CoV-2_Vaccination_Date_Duration_From_Index"].replace("N/A", -10000, inplace=True)
                curr_part["SARS-CoV-2_Vaccination_Date_Duration_From_Index"].replace("Not reported", "Not Reported", inplace=True)
                curr_part["SARS-CoV-2_Vaccination_Date_Duration_From_Index"].replace("Not Reported", -10000, inplace=True)
                curr_part = curr_part.sort_values(['SARS-CoV-2_Vaccination_Date_Duration_From_Index', "Visit_Number"],
                                                  ascending=[True, True])
            except Exception as e:
                print(e)
            data_db = pd.read_sql((f"SELECT Vaccination_Status FROM Covid_Vaccination_Status where Research_Participant_ID = '{curr_id}';"), sql_tuple[1])
            vacc_list = curr_part["Vaccination_Status"].tolist() + data_db["Vaccination_Status"].tolist()

            miss_d1 = "Dose 2 of 2" in vacc_list and "Dose 1 of 2" not in vacc_list
            miss_d2 = "Dose 3" in vacc_list and ("Dose 2 of 2" not in vacc_list and "Dose 2" not in vacc_list)
            miss_d2a = "Booster 1" in vacc_list and ("Dose 1 of 1" not in vacc_list and "Dose 2 of 2" not in vacc_list)
            curr_obj.add_miss_vac_errors(curr_part, curr_id, miss_d1, miss_d2, miss_d2a)

            index = [i for i in curr_part.index if ("Dose" in curr_part.loc[i, "Vaccination_Status"] or
                                                    "Booster" in curr_part.loc[i, "Vaccination_Status"])]
            if len(index) > 0:
                index = min(index)
                unvacc = curr_part.loc[index:].query("Vaccination_Status in ['Unvaccinated']")
                for err in unvacc.index:
                    error_msg = "Visit Number: " + str(unvacc.loc[err]["Visit_Number"]) + " is Unvaccinated. Particiapnt has prior vaccinae dosages"
                    curr_obj.add_error_values("Error", "Dosage_Errors.csv", int(err) + 1,
                                              "Research_Participant_ID", curr_id, error_msg)


def make_dict(curr_obj, master_dict, filt_table, type_var, cat_var, index):
    visit_num = (filt_table.iloc[index]["Visit_Number"])
    found_types = filt_table.iloc[index][type_var]
    found_status = filt_table.iloc[index][cat_var]
    visit_type = filt_table.iloc[index]["Visit_Type"]

    found_types = [i.strip() for i in found_types]
    found_status = [i.strip() for i in found_status]

    if visit_type == "Baseline":
        found_status = found_status*len(found_types)
        found_status = [i.replace("Yes", "New Condition") for i in found_status]
    if len(found_types) != len(found_status):  # this is an error that was previously caught
        return master_dict

    match_list = []
    extra_dict = {}
    for curr_type in enumerate(found_types):
        Status = found_status[curr_type[0]]
        if len(master_dict) == 0:  # no new records have been found previously
            if Status in ["New Condition"]:
                master_dict[curr_type[1]] = {"Visit_Number": visit_num, "Visit_Type": visit_type, "Status": Status}
        else:
            if curr_type[1] in master_dict and Status == 'Condition Resolved':
                del master_dict[curr_type[1]]
            elif curr_type[1] in master_dict:
                match_list.append(curr_type[1])
                z = filt_table.iloc[index]["Visit_Number"]
                master_dict[curr_type[1]]["Visit_Number"] = z
            elif curr_type[1] not in master_dict and Status == "New Condition":  # condtion is missing but is new
                master_dict[curr_type[1]] = {"Visit_Number": visit_num, "Visit_Type": visit_type, "Status": Status}
            elif "Not Reported" in found_status and "Not Reported" in found_types:
                pass  # not data was collected for this visit, can ignore
            else:
                extra_dict[curr_type[1]] = {"Visit_Number": visit_num, "Visit_Type": visit_type, "Status": Status}

    if len(extra_dict) > 0:
        for found in master_dict:
            for missing in extra_dict:
                found_visit = master_dict[found]["Visit_Number"]
                missing_visit = extra_dict[missing]["Visit_Number"]
                x = (f"For {cat_var}, condition: {found} was found at visit: {found_visit}, " +
                     f"but was not found at visit: {missing_visit}")
                if found_visit != missing_visit:
                    curr_obj.add_error_values("Error", "Cross_Sheet_Comobidity.csv", int(filt_table.loc[0]["index"]) + 2,
                                              "Research_Participant_ID", filt_table.loc[0]["Research_Participant_ID"], x)
    return master_dict


def check_baseline_date(current_object, pd, sql_tuple, parse):
    data_table = current_object.Data_Object_Table["baseline_visit_date.csv"]["Data_Table"]
    part_list = pd.read_sql(("SELECT Research_Participant_ID FROM Participant"), sql_tuple[1])

    missing = data_table.merge(part_list, indicator=True, how="left")
    has_data = missing.query("_merge in ['both']")
    missing = missing.query("_merge not in ['both']")

    sunday_logic = [parse(i).weekday() == 6 for i in has_data["Sunday_Of_Week"]]
    good_data = has_data[[i is True for i in sunday_logic]]
    bad_data = has_data[[i is False for i in sunday_logic]]

    for i in good_data.index:
        try:
            visit_day = parse(good_data["Sunday_Of_Week"][i]).date()
            part_id = good_data["Research_Participant_ID"][i]
            sql_query = (f"update `seronetdb-Vaccine_Response`.Participant set Sunday_Prior_To_First_Visit = '{visit_day}' " +
                        f"where Research_Participant_ID = '{part_id}'")
            sql_tuple[1].execute(sql_query)
        except Exception as e:
            print(e)
        finally:
            sql_tuple[2].connection.commit()
