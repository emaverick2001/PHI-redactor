import pandas as pd
import pprint
from typing import List, Iterable, Optional

from presidio_analyzer import BatchAnalyzerEngine, DictAnalyzerResult
from presidio_anonymizer import BatchAnonymizerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig

class ExcelAnalyzer(BatchAnalyzerEngine):
    """
    ExcelAnalyzer class.

    A class that provides functionality to analyze excel files.
    """

    def analyze_excel(
    self,
    excel_full_path: str,
    language: str,
    keys_to_skip: Optional[List[str]] = None, # NLP artifacts to skip/ keys to skip in the analysis
    **kwargs,
    ) -> List[DictAnalyzerResult]:

        df = pd.read_excel(excel_full_path)
        results = []

        for index, row in df.iterrows():
            row_dict = row.to_dict()
            # Convert datetime values to strings and handle NaT values
            for key, value in row_dict.items():
                if isinstance(value, pd.Timestamp):
                    row_dict[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif pd.isnull(value):
                    row_dict[key] = None  # or some default value
            analyzer_results = self.analyze_dict(row_dict, language, keys_to_skip)
            results.extend(list(analyzer_results))

        return results
        
    def anonymize_and_save_excel(
        self,
        excel_full_path: str,
        output_path: str,
        language: str,
        keys_to_skip: Optional[List[str]] = None,
        **kwargs,
    ) -> None:

        if excel_full_path == output_path:
            raise ValueError("Output path cannot be the same as the input path.")

        df = pd.read_excel(excel_full_path)
        anonymizer_engine = AnonymizerEngine()
        # anonymizer_engine.add_recognizer(PersonRecognizer())

        for index, row in df.iterrows():
            row_dict = row.to_dict()
            # Convert datetime values to strings and handle NaT values
            for key, value in row_dict.items():
                if isinstance(value, pd.Timestamp):
                    row_dict[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif pd.isnull(value):
                    row_dict[key] = None  # or some default value
                    continue  # Skip None values
                elif isinstance(value, float) or isinstance(value, int):
                    row_dict[key] = str(value)  # Convert float or int to string
            dict_analyzer_results = self.analyze_dict(row_dict, language, keys_to_skip)
            
            for dict_result in dict_analyzer_results:
                # Skip None values
                if row_dict[dict_result.key] is None:
                    continue
                # Prepare anonymizer configuration
                operators = {result.entity_type: OperatorConfig("replace", {"new_value": "<ANONYMIZED>"}) for result in dict_result.recognizer_results}
                anonymized_text = anonymizer_engine.anonymize(text=row_dict[dict_result.key], analyzer_results=dict_result.recognizer_results, operators=operators)
                
                # Replace original values with anonymized values
                df.loc[index, dict_result.key] = anonymized_text

        df.to_excel(output_path, index=False)


if __name__ == "__main__":

    analyzer = ExcelAnalyzer()
    columns_to_skip = ['Initials','Primary Guide','Assistant Guide','Race or Ethnicity']
    results = analyzer.anonymize_and_save_excel('labdata.xlsx', 'anonymized_labdata.xlsx', language="en", keys_to_skip=columns_to_skip, return_decision_process=True)
    
    df = pd.read_excel('anonymized_labdata.xlsx')
    
    # After the anonymization process
    for column in df.columns:
        df[column] = df[column].str.replace('text: ', '', regex=False)
        df[column] = df[column].str.split('\n', expand=True)[0]
    
    # Save the modified DataFrame back to the Excel file
    df.to_excel('anonymized_labdata.xlsx', index=False)
    
    # Prints the results of the decision process
    # decision_process = results['decision_process'].analyze_decision
    # pp = pprint.PrettyPrinter()
    # print("Anonymization decision process:\n")
    # pp.pprint(decision_process.__dict__)
    
    # This is just returning raw results of the analyzer and anonymizer not implemented into the data file
    # analyzer_results = analyzer.analyze_excel('labdata.xlsx', language="en")
    # pprint.pprint(analyzer_results)

    # anonymizer = BatchAnonymizerEngine()
    # anonymized_results = anonymizer.anonymize_dict(analyzer_results)
    # pprint.pprint(anonymized_results)

