import re
from constants import NIFTY_FIFTY_TABLE, TRADE_DATE, logger, APPLIED_ACTIONS_LOG, DUCKDB_PATH, SYMBOL, TRADE_DATE, NIFTY_FIFTY_TABLE
from datetime import datetime
from duckdb_manager import DuckDBManager

class GeneralMeeting():
    def __init__(self, con):
        self.con = con
        self.price_columns = ["OPEN", "HIGH", "LOW", "CLOSE", "LAST", "PREVCLOSE"]
        self._init_actions_log_table()

    def _init_actions_log_table(self):
        """
        Creates the log table if it doesn't already exist.
        """
        create_log_table_query = f"""
        CREATE TABLE IF NOT EXISTS {APPLIED_ACTIONS_LOG} (
            log_id UUID PRIMARY KEY,
            symbol VARCHAR,
            exec_date DATE,
            action_type VARCHAR,
            action_details VARCHAR,
            adjustment_factor DOUBLE,
            applied_timestamp TIMESTAMP
        );
        """
        self.con.execute(create_log_table_query)
        print(f"Ensured '{APPLIED_ACTIONS_LOG}' table exists.")


    def _log_action(self, action):
        """Logs the details of an applied action to the log table."""
        log_query = f"""
        INSERT INTO {APPLIED_ACTIONS_LOG} VALUES (uuid(), ?, ?, ?, ?, ?, ?);
        """
        self.con.execute(log_query, [
            action['symbol'], action['exec_date'], action['action_type'],
            action['action_details'], action['adjustment_factor'], datetime.now()
        ])
        print(f"Logged action for {action['symbol']} on {action['exec_date']}.")


    def get_ratio_and_exec_date(self, symbol: str, exec_date: str, purpose: str):
        final_resp = dict()
        purpose = purpose.strip().upper()

        if "FACE VALUE SPLIT" in purpose:
            from_match = re.search(
            r"(?:FROM\s+)?R(?:S|E)\.?\s*(\d+(?:\.\d+)?)", purpose, flags=re.IGNORECASE
            )
            to_match = re.search(
                r"TO\s+R(?:S|E)\.?\s*(\d+(?:\.\d+)?)", purpose, flags=re.IGNORECASE
            )
            if from_match and to_match:
                from_price = float(from_match.group(1))
                to_price = float(to_match.group(1))
                final_resp['face_split'] = f"{from_price}:{to_price}"
                final_resp['split_factor'] = to_price / from_price
            
            else:
                print(f"Unable to find the split values -> {purpose}")
        
        if "BONUS" in purpose:
            bonus_match = re.search(r"BONUS(?:\s+DEBENTURES)?\s+(\d+)\s*:\s*(\d+)", purpose)
            if bonus_match:
                new_shares = int(bonus_match.group(1))
                old_shares = int(bonus_match.group(2))
                bonus_ratio = f"{new_shares}:{old_shares}"
                final_resp['bonus'] = bonus_ratio
                final_resp['bonus_factor'] = old_shares / (old_shares + new_shares)
            else:
                print(f"Unable to find the bonus ratio -> {purpose}")

        if "RIGHTS" in purpose:
            query = f"SELECT close FROM {NIFTY_FIFTY_TABLE} WHERE {SYMBOL} = ? AND {TRADE_DATE} < ? ORDER BY {TRADE_DATE} DESC LIMIT 1"
            result = self.con.execute(query, [symbol, exec_date]).fetchone()
            if not result:
                print(f"Unable to fetch price previous to {exec_date} for {symbol} -> {purpose}")
                return None

            right_match = re.search(r"RIGHTS(?:-EQ)?\s*(\d+)\s*:\s*(\d+)", purpose)
            price_match = re.search(r"(?:@PREM|@PREMIUM|@ PREMIUM)?\s*R(?:S|E)\.?\s*(\d+(?:\.\d+)?)", purpose, flags=re.IGNORECASE)
            
            if right_match and price_match:
                right_ratio = f"{right_match.group(1)}:{right_match.group(2)}"
                final_resp['rights'] = right_ratio
                rights_price = float(price_match.group(1))
                final_resp["rights_price"] = rights_price

                if not rights_price or not right_ratio:
                    print(f"Unable to fetch price previous to {exec_date} for {symbol} -> {purpose}")
                    return None

                cum_rights_price = result[0]

                if cum_rights_price == 0:
                    print(f"Warning: Prior closing price for {symbol} is zero. Skipping rights adjustment.")
                    return None

                final_resp['last_day_stock_price_from_exec_date'] = cum_rights_price
                new_shares, old_shares = map(int, right_ratio.split(':'))
                ex_rights_price = ((cum_rights_price * old_shares) + (rights_price * new_shares)) / (old_shares + new_shares)
                final_resp['rights_factor'] = ex_rights_price / cum_rights_price
            else:
                self.get_blended_rights(purpose, final_resp, result)
        
        final_resp['exec_date'] = exec_date
        final_resp['symbol'] = symbol
        return final_resp
    
    def get_blended_rights(self, purpose, final_resp, result):
        pattern = r"(\d+):(\d+).*?@.*?RS\s*(\d+(?:\.\d+)?)"
        matches = re.findall(pattern, purpose)
        if not matches:
            print(f"Could not parse the complex rights issue -> {purpose}")
            return None
        else:
            rights_issues = []
            blended_rights = []
            for match in matches:
                rights_issues.append({
                    'new_shares': int(match[0]),
                    'old_shares_basis': int(match[1]), # The 'B' in A:B
                    'price': float(match[2])
                })
            for rights_issue in rights_issues:
                blended_rights.append(f"{rights_issue['new_shares']}:{rights_issue['old_shares_basis']} at {rights_issue['price']}")

            cum_rights_price = result[0]
            final_resp['blended_rights'] = ", ".join(blended_rights)
            final_resp['blended_rights_factor'] = self.calculate_blended_rights_factor(rights_issues, cum_rights_price)
            final_resp['last_day_stock_price_from_exec_date'] = cum_rights_price

    def calculate_blended_rights_factor(self, rights_issues, cum_rights_price):
        def lcm(a, b):
            from math import gcd
            return abs(a * b) // gcd(a, b) if a and b else 0
        
        if not rights_issues or cum_rights_price == 0:
            return 1.0

        # Step 1: Find the common basis using LCM
        denominators = [issue['old_shares_basis'] for issue in rights_issues]
        if not denominators:
            return 1.0
            
        common_denominator = denominators[0]
        for i in range(1, len(denominators)):
            common_denominator = lcm(common_denominator, denominators[i])

        # If LCM is 0 (e.g., from a 0 basis), cannot proceed
        if common_denominator == 0:
            return 1.0

        # Step 2: Scale the offers and calculate total new shares and cost
        total_cost_of_new_shares = 0
        total_number_of_new_shares = 0
        
        for issue in rights_issues:
            scaling_factor = common_denominator // issue['old_shares_basis']
            scaled_new_shares = issue['new_shares'] * scaling_factor
            
            total_number_of_new_shares += scaled_new_shares
            total_cost_of_new_shares += scaled_new_shares * issue['price']

        # Step 3: Proceed with the blended calculation
        original_value = common_denominator * cum_rights_price
        total_shares = common_denominator + total_number_of_new_shares
        total_value = original_value + total_cost_of_new_shares

        if total_shares == 0:
            return 1.0

        ex_rights_price = total_value / total_shares
        adjustment_factor = ex_rights_price / cum_rights_price
        
        return adjustment_factor
    
    def get_exec_date(self, ex_date: str):
        try:
            ex_date = ex_date.strip().replace('"', '')
            date_obj = datetime.strptime(ex_date, "%d-%b-%Y")
            return date_obj.strftime("%Y-%m-%d")
        except Exception as e:
            logger.info(f"Unable to parse date {ex_date}")
        

    def get_split_or_bonus_details(self, csv_line: str):

        splitted_lines = csv_line.split(",")
        
        symbol = splitted_lines[0].replace('"', '')
        purpose = splitted_lines[3]
        ex_date = self.get_exec_date(splitted_lines[5])
        split_details = self.get_ratio_and_exec_date(symbol, ex_date, purpose)
        if not split_details or not ex_date or not symbol:
            return False

        return split_details


    def get_actions_from_csv(self, file_path):
        details = []
        with open(file_path, 'r') as f:
            for i, line in enumerate(f):
                if i==0:
                    continue
                detail = self.get_split_or_bonus_details(line)
                if detail and ('rights_factor' in detail or 
                                'bonus_factor' in detail or 
                                'split_factor' in detail or 
                                'blended_rights_factor' in detail):
                    details.append(detail)
        return details


    def check_if_adjustment_already_done(self, action: dict):
        check_log_query = f"""
                            SELECT 1 FROM {APPLIED_ACTIONS_LOG}
                            WHERE symbol = ? AND exec_date = ? AND action_type = ?
                            """
        
        log_exists = self.con.execute(check_log_query, [
            action['symbol'], 
            action['exec_date'],
            action['action_type']
        ]).fetchone()

        if log_exists:
            print(f"Action '{action['action_type']}' for {action['symbol']} on {action['exec_date']} has already been applied. Skipping.")
            return True
        
        return False


    def check_if_data_exists(self, action: dict):
        record_exists_query = f"SELECT * FROM {NIFTY_FIFTY_TABLE} WHERE {SYMBOL} = ? AND {TRADE_DATE} < ?"
        record_exists_result = self.con.execute(record_exists_query, [action['symbol'], action['exec_date']]).fetchone()
        return record_exists_result
    

    def confirm_action(self, action: dict):
        print("\n" + "="*50)
        print(f"Action required for:              {action['symbol']}")
        print(f"Execution Date:                   {action['exec_date']}")
        print(f"Action Type:                      {action['action_type']}")
        print(f"Details:                          {action['action_details']}")
        print(f"Calculated Adjustment Factor:     {action['adjustment_factor']:.6f}")
        print("This will multiply all historical prices (Open, High, Low, Close, etc.)")
        print(f"for {action['symbol']} before {action['exec_date']} by the factor above.")
        print("="*50)
        
        confirm = input("Do you want to apply this adjustment? (y/n): ").lower().strip()

        return confirm

    def update_table(self, action: dict):
        print(f"Applying adjustment for {action['symbol']}...")
        set_clauses = [f"{col.lower()} = {col.lower()} * ?" for col in self.price_columns]
        update_query = f"UPDATE {NIFTY_FIFTY_TABLE} SET {', '.join(set_clauses)} WHERE {SYMBOL} = ? AND {TRADE_DATE} < ?"
        params = [action['adjustment_factor']] * len(self.price_columns) + [action['symbol'], action['exec_date']]

        # Print the update query and parameters before execution
        print(f"Executing query: {update_query}")
        print(f"With parameters: {params}")
        self.con.execute(update_query, params)
 

    def process_and_confirm_actions(self, actions_data):
        sorted_actions = sorted(actions_data, key=lambda x: x.get('exec_date', ''))

        for action in sorted_actions:
            # Combine factors
            base_factor = 1.0
            action_types = []
            action_details = []

            if 'split_factor' in action:
                base_factor *= action['split_factor']
                action_types.append('Face Split')
                action_details.append(f"Face Split: {action['face_split']}")
            if 'bonus_factor' in action:
                base_factor *= action['bonus_factor']
                action_types.append('Bonus')
                action_details.append(f"Bonus: {action['bonus']}")
            if 'rights_factor' in action:
                base_factor *= action['rights_factor']
                action_types.append('Rights')
                action_details.append(f"Rights: {action['rights']} (based on close price {action.get('last_day_stock_price_from_exec_date', 'N/A')})")
            if 'blended_rights_factor' in action:
                base_factor *= action['blended_rights_factor']
                action_types.append('Blended Rights')
                action_details.append(f"Blended Rights: {action['blended_rights']} (based on close price {action.get('last_day_stock_price_from_exec_date', 'N/A')})")
            # Skip if no action was parsed or factor is 1
            if not action_types or abs(base_factor - 1.0) < 1e-9:
                continue

            action['adjustment_factor'] = base_factor
            action['action_type'] = " & ".join(action_types)
            action['action_details'] = ", ".join(action_details)

            if self.check_if_adjustment_already_done(action):
                continue
            
            if not self.check_if_data_exists(action):
                print(f"No records found for {action['symbol']} before {action['exec_date']}")
                continue

            confirm = self.confirm_action(action)
            if confirm == 'y':
                self.update_table(action)
                self._log_action(action)


    def adjust_price(self, file_path):
        actions_data = self.get_actions_from_csv(file_path)
        self.process_and_confirm_actions(actions_data)
        


