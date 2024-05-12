import json
import os

class json_file_df:
    def agg_transactions(self):
        path = 'D:/PERSONAL/TRAINING/GUVI/PYTHON/phonepe_pulse/data/aggregated/transaction/country/india/state'
        agg_list_ctry = os.listdir(path)
        agt = []

        for sts in agg_list_ctry:
            path_sts = os.path.join(path, sts + "/")
            agt_year = os.listdir(path_sts)
            for yrs in agt_year:
                path_yrs = path_sts + yrs + "/"
                agt_qtr = os.listdir(path_yrs)
                for qtr in agt_qtr:
                    path_data = path_yrs + qtr
                    Data = open(path_data, "r")
                    json_data = json.load(Data)
                    for res_data in json_data["data"]["transactionData"]:
                        Transaction_Type = res_data["name"]
                        Transaction_Count = res_data["paymentInstruments"][0]["count"]
                        Transaction_Amount = res_data["paymentInstruments"][0]["amount"]
                        agt.append({"State": sts, "Year": yrs, "Quarter": int(qtr.strip('.json')),
                                    "Transaction_Type": Transaction_Type,
                                    "Total_Transactions": Transaction_Count, "Transaction_Amount": Transaction_Amount})
        if agt:
            return agt
        else:
            return None

    def agg_users(self):
        path = 'D:/PERSONAL/TRAINING/GUVI/PYTHON/phonepe_pulse/data/aggregated/user/country/india/state'
        agg_list_ctry = os.listdir(path)
        agu = []

        for sts in agg_list_ctry:
            path_sts = os.path.join(path, sts + "/")
            agu_year = os.listdir(path_sts)
            for yrs in agu_year:
                path_yrs = os.path.join(path_sts, yrs + "/")
                agu_qtr = os.listdir(path_yrs)
                for qtr in agu_qtr:
                    path_data = os.path.join(path_yrs, qtr)
                    try:
                        Data = open(path_data, "r")
                        json_data = json.load(Data)
                        if json_data and json_data.get("data") and json_data["data"].get("usersByDevice"):
                            for res_data in json_data["data"]["usersByDevice"]:
                                Device_Brand = res_data["brand"]
                                User_Count = res_data["count"]
                                Device_Share_Percentage = res_data["percentage"]
                                agu.append({"State": sts, "Year": yrs, "Quarter": int(qtr.strip('.json')),
                                            "Device_Brand": Device_Brand,
                                            "User_Count": User_Count,
                                            "Device_Share_Percentage": Device_Share_Percentage})
                        else:
                            pass
                    except Exception as e:
                        # print(f"Error reading JSON data from file {path_data}: {e}")
                        pass

        if agu:
            return agu
        else:
            return None

    def map_users(self):
        path = 'D:/PERSONAL/TRAINING/GUVI/PYTHON/phonepe_pulse/data/map/user/hover/country/india/state'
        map_list_ctry = os.listdir(path)
        mpu = []

        for sts in map_list_ctry:
            path_sts = os.path.join(path, sts + "/")
            mpu_year = os.listdir(path_sts)
            for yrs in mpu_year:
                path_yrs = os.path.join(path_sts, yrs + "/")
                mpu_qtr = os.listdir(path_yrs)
                for qtr in mpu_qtr:
                    path_data = os.path.join(path_yrs, qtr)
                    try:
                        Data = open(path_data, "r")
                        json_data = json.load(Data)
                        for res_data in json_data["data"]["hoverData"].items():
                            District = res_data[0]
                            Total_Apps = res_data[1]["appOpens"]
                            User_Count = res_data[1]["registeredUsers"]
                            mpu.append(
                                {"State": sts, "Year": yrs, "Quarter": int(qtr.strip('.json')), "District": District,
                                 "User_Count": User_Count, "Total_Used_Apps": Total_Apps})

                    except Exception as e:
                        # print(f"Error reading JSON data from file {path_data}: {e}")
                        pass

        if mpu:
            return mpu
        else:
            return None

    def map_transactions(self):
        path = 'D:/PERSONAL/TRAINING/GUVI/PYTHON/phonepe_pulse/data/map/transaction/hover/country/india/state'
        map_list_ctry = os.listdir(path)
        mpt = []

        for sts in map_list_ctry:
            path_sts = os.path.join(path, sts + "/")
            mpt_year = os.listdir(path_sts)
            for yrs in mpt_year:
                path_yrs = os.path.join(path_sts, yrs + "/")
                mpt_qtr = os.listdir(path_yrs)
                for qtr in mpt_qtr:
                    path_data = os.path.join(path_yrs, qtr)
                    try:
                        Data = open(path_data, "r")
                        json_data = json.load(Data)
                        for res_data in json_data["data"]["hoverDataList"]:
                            District = res_data["name"]
                            Total_Transactions = res_data["metric"][0]["count"]
                            Total_Amount = res_data["metric"][0]["amount"]
                            mpt.append(
                                {"State": sts, "Year": yrs, "Quarter": int(qtr.strip('.json')), "District": District,
                                 "Total_Transactions": Total_Transactions, "Transaction_Amount": Total_Amount})

                    except Exception as e:
                        # print(f"Error reading JSON data from file {path_data}: {e}")
                        pass

        if mpt:
            return mpt
        else:
            return None

    def top_transactions(self):
        path = r'D:\PERSONAL\TRAINING\GUVI\PYTHON\phonepe_pulse\data\top\transaction\country\india\state'
        top_list_ctry = os.listdir(path)

        tpt_districts = []
        tpt_pincodes = []

        for sts in top_list_ctry:
            path_sts = os.path.join(path, sts + "/")
            tpt_year = os.listdir(path_sts)
            for yrs in tpt_year:
                path_yrs = os.path.join(path_sts, yrs + "/")
                tpt_qtr = os.listdir(path_yrs)
                for qtr in tpt_qtr:
                    path_data = os.path.join(path_yrs, qtr)
                    try:
                        Data = open(path_data, "r")
                        json_data = json.load(Data)
                        if json_data and json_data.get("data") and json_data["data"].get("districts"):
                            for res_data in json_data["data"]["districts"]:
                                District_Name = res_data["entityName"]
                                Transaction_Count = res_data["metric"]["count"]
                                Transaction_Amount = res_data["metric"]["amount"]
                                tpt_districts.append({"State": sts, "Year": yrs, "Quarter": int(qtr.strip('.json')),
                                                      "District_Name": District_Name,
                                                      "Total_Transactions": Transaction_Count,
                                                      "Transaction_Amount": Transaction_Amount})
                        else:
                            pass
                        if json_data and json_data.get("data") and json_data["data"].get("pincodes"):
                            for res_data in json_data["data"]["pincodes"]:
                                Pincode = res_data["entityName"]
                                Transaction_Count = res_data["metric"]["count"]
                                Transaction_Amount = res_data["metric"]["amount"]
                                tpt_pincodes.append(
                                    {"State": sts, "Year": yrs, "Quarter": int(qtr.strip('.json')), "Pincode": Pincode,
                                     "Total_Transactions": Transaction_Count, "Transaction_Amount": Transaction_Amount})
                        else:
                            pass
                    except Exception as e:
                        # print(f"Error reading JSON data from file {path_data}: {e}")
                        pass

        if tpt_pincodes:
            return tpt_pincodes
        else:
            return None

    def top_users(self):
        path = r'D:\PERSONAL\TRAINING\GUVI\PYTHON\phonepe_pulse\data\top\user\country\india\state'
        top_list_ctry = os.listdir(path)
        tpu_districts = []
        tpu_pincodes = []

        for sts in top_list_ctry:
            path_sts = os.path.join(path, sts + "/")
            tpu_year = os.listdir(path_sts)
            for yrs in tpu_year:
                path_yrs = os.path.join(path_sts, yrs + "/")
                tpu_qtr = os.listdir(path_yrs)
                for qtr in tpu_qtr:
                    path_data = os.path.join(path_yrs, qtr)
                    try:
                        Data = open(path_data, "r")
                        json_data = json.load(Data)
                        if json_data and json_data.get("data") and json_data["data"].get("districts"):
                            for res_data in json_data["data"]["districts"]:
                                District_Name = res_data["name"]
                                User_Count = res_data["registeredUsers"]
                                tpu_districts.append({"State": sts, "Year": yrs, "Quarter": int(qtr.strip('.json')),
                                                      "District_Name": District_Name,
                                                      "User_Count": User_Count})
                        else:
                            pass
                        if json_data and json_data.get("data") and json_data["data"].get("pincodes"):
                            for res_data in json_data["data"]["pincodes"]:
                                Pincode = res_data["name"]
                                User_Count = res_data["registeredUsers"]
                                tpu_pincodes.append(
                                    {"State": sts, "Year": yrs, "Quarter": int(qtr.strip('.json')), "Pincode": Pincode,
                                     "User_Count": User_Count})
                        else:
                            pass
                    except Exception as e:
                        # print(f"Error reading JSON data from file {path_data}: {e}")
                        pass

        if tpu_pincodes:
            return tpu_pincodes
        else:
            return None

