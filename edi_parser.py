
from langchain_openai import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import json
import re
import logging

logger = logging.getLogger("EDIParser")

class EDIParser:
    """
    EDI Parser using LLM or direct parsing
    """
    def __init__(self, api_key=""):
        self.api_key = api_key
        if api_key:
            self.llm = OpenAI(temperature=0, api_key=api_key)
            
            # Define a simple prompt template for EDI parsing instead of using the agent
            self.template = """
            You are an expert EDI (Electronic Data Interchange) analyst specializing in parsing and mapping 
            EDI 944 (Warehouse Stock Transfer Receipt) data to JSON format. You need to handle various 
            formats and implementations of the 944 standard.

            Here's the EDI 944 structure specification:
            - ISA/GS/ST: EDI envelope segments (not part of 944 specific data but always present)
            - 944 Header Segments:
              - W17 (Mandatory, Pos 020): Warehouse Receipt Identification with format:
                W17*[receiptType]*[date]*[receiptNumber]*[shipmentNumber]*[containerNumber]*[numberOfLines]*[totalQuantity]
              - LOOP ID - N1:
                - N1 (Mandatory, Pos 040): Name with format:
                  N1*[entityIdentifier]*[name]
                - N9 (Optional, Pos 090): Reference Identification with format:
                  N9*[referenceIdQualifier]*[referenceId]
            - 944 Detail Segments:
              - LOOP ID - W07 (Can repeat):
                - W07 (Mandatory, Pos 020): Item Detail For Stock Receipt with format:
                  W07*[quantity]*[unitOfMeasure]*[upc]*[productIdQualifier]*[productId]
                - G69 (Optional, Pos 030): Line Item Detail - Description with format:
                  G69*[description]
                - N9 (Optional): Reference Identification related to this W07 item
            - 944 Summary Segments:
              - W13 or W14 (Mandatory, Pos 110): Total Receipt Information with format:
                W13*[totalQuantity] or W14*[totalQuantity]
            - SE/GE/IEA: EDI closing envelope segments

            The EDI data uses ~ as a segment delimiter and * as an element delimiter.

            Your task is to extract only the meaningful 944 transaction data, ignoring envelope segments (ISA, GS, ST, SE, GE, IEA).

            For any segment format you encounter:
            1. Parse all W17 segment data for the header
            2. Associate all N1 segments with their related N9 segments in the header
            3. For each W07 segment, associate it with any G69 and related N9 segments that follow it
            4. Include the W13 or W14 summary segment

            The output should be a valid JSON object with this structure:
            ```json
            {
              "transactionSet": "944",
              "header": {
                "W17": {
                  "receiptType": "value",
                  "date": "value",
                  "receiptNumber": "value",
                  "shipmentNumber": "value",
                  "containerNumber": "value",
                  "numberOfLines": "value",
                  "totalQuantity": "value"
                },
                "N1Loop": [
                  {
                    "N1": {
                      "entityIdentifier": "value",
                      "name": "value"
                    },
                    "N9": [
                      {
                        "referenceIdQualifier": "value",
                        "referenceId": "value"
                      }
                    ]
                  }
                ]
              },
              "detail": {
                "W07Loop": [
                  {
                    "W07": {
                      "quantity": "value",
                      "unitOfMeasure": "value",
                      "upc": "value",
                      "productIdQualifier": "value",
                      "productId": "value"
                    },
                    "G69": "value",
                    "N9": [
                      {
                        "referenceIdQualifier": "value",
                        "referenceId": "value"
                      }
                    ]
                  }
                ]
              },
              "summary": {
                "W13": {
                  "totalQuantity": "value"
                }
              }
            }
            ```

            Important notes:
            - If W14 is used instead of W13, place it under "summary" as "W14" rather than "W13"
            - If any segment is missing or has fewer elements than expected, include empty strings for missing values
            - Make sure to correctly associate each N9 segment with either its parent N1 segment or its parent W07 segment
            - Some EDI messages might contain additional segment types - focus only on the ones relevant to 944

            Please analyze the EDI data provided below and return ONLY the JSON object:

            {edi_data}
            """
            
            self.prompt = PromptTemplate(
                template=self.template,
                input_variables=["edi_data"]
            )
            
            self.chain = LLMChain(llm=self.llm, prompt=self.prompt)
    
    def parse(self, edi_data):
        """
        Parse EDI data and convert to proper JSON format
        """
        # First, let's clean up the EDI data
        cleaned_edi = self._clean_edi_data(edi_data)
        
        if not self.api_key:
            # If no API key, use direct parser
            return self._direct_parser(cleaned_edi)
            
        try:
            # Use LangChain to parse with LLM
            result = self.chain.invoke({"edi_data": cleaned_edi})
            
            # The result structure might be different based on LangChain version
            if isinstance(result, dict) and "text" in result:
                output = result["text"]
            else:
                output = str(result)
            
            # Try to extract JSON from the output
            json_match = re.search(r'```json\n(.*?)\n```', output, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
                parsed_result = json.loads(json_str)
                if self._validate_result(parsed_result):
                    return parsed_result
            
            # Try to find any JSON structure in the output
            json_match = re.search(r'\{[\s\S]*\}', output)
            if json_match:
                try:
                    parsed_result = json.loads(json_match.group(0))
                    if self._validate_result(parsed_result):
                        return parsed_result
                except:
                    logger.warning("Found JSON-like structure but couldn't parse it")
            
            # If no JSON found or validation failed, use direct parser
            logger.warning("No valid JSON found in LLM output, using direct parser")
            return self._direct_parser(cleaned_edi)
            
        except Exception as e:
            logger.error(f"Error using LLM parser: {str(e)}")
            # Fall back to direct parser
            return self._direct_parser(cleaned_edi)
    
    def _clean_edi_data(self, edi_data):
        """Clean up EDI data by removing extra whitespace and normalizing delimiters"""
        if not edi_data:
            return ""
            
        # Normalize line endings and remove extra spaces
        cleaned = re.sub(r'\r\n|\r|\n', '', edi_data)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Ensure segments end with ~ if they don't already
        if not cleaned.endswith('~'):
            cleaned += '~'
            
        return cleaned
    
    def _validate_result(self, result):
        """Validate that the parsed result has the expected structure"""
        if not isinstance(result, dict):
            return False
            
        # Check for required top-level keys
        required_keys = ["transactionSet", "header", "detail", "summary"]
        if not all(key in result for key in required_keys):
            return False
            
        # Check for required header data
        if "W17" not in result.get("header", {}):
            return False
            
        # Check for W07Loop
        if "W07Loop" not in result.get("detail", {}):
            return False
            
        return True
    
    def _direct_parser(self, edi_data):
        """
        A direct parser for EDI 944 data without using LLM
        """
        logger.info("Using direct parser for EDI data")
        
        # Create the base JSON structure
        json_structure = {
            "transactionSet": "944",
            "header": {},
            "detail": {
                "W07Loop": []
            },
            "summary": {}
        }
        
        if not edi_data:
            return json_structure
        
        # Split by segment delimiter and filter out empty segments
        segments = [s for s in edi_data.strip().split("~") if s]
        
        # Context tracking variables
        current_section = None
        current_w07 = None
        current_n1 = None
        
        for segment in segments:
            # Split segment by element delimiter
            elements = segment.split("*")
            if not elements:
                continue
                
            segment_type = elements[0]
            
            # Transaction set header
            if segment_type == "ST" and len(elements) > 1 and elements[1] == "944":
                json_structure["transactionSet"] = "944"
                current_section = "header"
                
            # Header section - W17
            elif segment_type == "W17":
                current_section = "header"
                if len(elements) >= 7:
                    json_structure["header"]["W17"] = {
                        "receiptType": elements[1] if len(elements) > 1 else "",
                        "date": elements[2] if len(elements) > 2 else "",
                        "receiptNumber": elements[3] if len(elements) > 3 else "",
                        "shipmentNumber": elements[4] if len(elements) > 4 else "",
                        "containerNumber": elements[5] if len(elements) > 5 else "",
                        "numberOfLines": elements[6] if len(elements) > 6 else "",
                        "totalQuantity": elements[7] if len(elements) > 7 else ""
                    }
            
            # N1 Loop
            elif segment_type == "N1":
                current_section = "header"
                if "N1Loop" not in json_structure["header"]:
                    json_structure["header"]["N1Loop"] = []
                
                current_n1 = {
                    "N1": {
                        "entityIdentifier": elements[1] if len(elements) > 1 else "",
                        "name": elements[2] if len(elements) > 2 else ""
                    },
                    "N9": []
                }
                
                json_structure["header"]["N1Loop"].append(current_n1)
            
            # N9 in N1 Loop or as standalone after N1
            elif segment_type == "N9":
                # If we're in the header section and have a current N1, associate with it
                if current_section == "header" and current_n1 is not None:
                    n9_entry = {
                        "referenceIdQualifier": elements[1] if len(elements) > 1 else "",
                        "referenceId": elements[2] if len(elements) > 2 else ""
                    }
                    current_n1["N9"].append(n9_entry)
                # If we're in the detail section with a current W07, associate with it
                elif current_section == "detail" and current_w07 is not None:
                    n9_entry = {
                        "referenceIdQualifier": elements[1] if len(elements) > 1 else "",
                        "referenceId": elements[2] if len(elements) > 2 else ""
                    }
                    current_w07["N9"].append(n9_entry)
                # If we have no current context but have a header with N1Loop, add to the last N1
                elif "N1Loop" in json_structure.get("header", {}) and json_structure["header"]["N1Loop"]:
                    n9_entry = {
                        "referenceIdQualifier": elements[1] if len(elements) > 1 else "",
                        "referenceId": elements[2] if len(elements) > 2 else ""
                    }
                    json_structure["header"]["N1Loop"][-1]["N9"].append(n9_entry)
            
            # W07 Loop starts
            elif segment_type == "W07":
                current_section = "detail"
                current_w07 = {
                    "W07": {
                        "quantity": elements[1] if len(elements) > 1 else "",
                        "unitOfMeasure": elements[2] if len(elements) > 2 else "",
                        "upc": elements[3] if len(elements) > 3 else "",
                        "productIdQualifier": elements[4] if len(elements) > 4 else "",
                        "productId": elements[5] if len(elements) > 5 else ""
                    },
                    "N9": []
                }
                
                json_structure["detail"]["W07Loop"].append(current_w07)
            
            # G69 in W07 Loop
            elif segment_type == "G69" and current_section == "detail" and current_w07 is not None:
                # G69 has the description in the first element after the segment type
                current_w07["G69"] = elements[1] if len(elements) > 1 else ""
            
            # Summary section - W13 or W14
            elif segment_type in ["W13", "W14"]:
                current_section = "summary"
                json_structure["summary"][segment_type] = {
                    "totalQuantity": elements[1] if len(elements) > 1 else ""
                }
        
        # Do a final validation check
        if not json_structure["header"] and "W17" in [s.split("*")[0] for s in segments if "*" in s]:
            # We have a W17 segment but didn't parse it correctly - try again with more permissive parsing
            w17_segment = next((s for s in segments if s.startswith("W17*")), None)
            if w17_segment:
                elements = w17_segment.split("*")
                json_structure["header"]["W17"] = {
                    "receiptType": elements[1] if len(elements) > 1 else "",
                    "date": elements[2] if len(elements) > 2 else "",
                    "receiptNumber": elements[3] if len(elements) > 3 else "",
                    "shipmentNumber": elements[4] if len(elements) > 4 else "",
                    "containerNumber": elements[5] if len(elements) > 5 else "",
                    "numberOfLines": elements[6] if len(elements) > 6 else "",
                    "totalQuantity": elements[7] if len(elements) > 7 else ""
                }
        
        # Make sure we have N1Loop if there's an N1 segment
        if "N1Loop" not in json_structure["header"] and "N1" in [s.split("*")[0] for s in segments if "*" in s]:
            n1_segment = next((s for s in segments if s.startswith("N1*")), None)
            if n1_segment:
                elements = n1_segment.split("*")
                json_structure["header"]["N1Loop"] = [{
                    "N1": {
                        "entityIdentifier": elements[1] if len(elements) > 1 else "",
                        "name": elements[2] if len(elements) > 2 else ""
                    },
                    "N9": []
                }]
                
                # Find any N9 segments and associate with this N1
                for segment in segments:
                    if segment.startswith("N9*"):
                        elements = segment.split("*")
                        n9_entry = {
                            "referenceIdQualifier": elements[1] if len(elements) > 1 else "",
                            "referenceId": elements[2] if len(elements) > 2 else ""
                        }
                        json_structure["header"]["N1Loop"][0]["N9"].append(n9_entry)
        
        # If W07Loop is empty but we have W07 segments, try to parse them
        if not json_structure["detail"]["W07Loop"] and "W07" in [s.split("*")[0] for s in segments if "*" in s]:
            # Find all W07 segments
            w07_segments = [s for s in segments if s.startswith("W07*")]
            
            for w07_segment in w07_segments:
                elements = w07_segment.split("*")
                w07_item = {
                    "W07": {
                        "quantity": elements[1] if len(elements) > 1 else "",
                        "unitOfMeasure": elements[2] if len(elements) > 2 else "",
                        "upc": elements[3] if len(elements) > 3 else "",
                        "productIdQualifier": elements[4] if len(elements) > 4 else "",
                        "productId": elements[5] if len(elements) > 5 else ""
                    },
                    "N9": []
                }
                
                # Find the next G69 segment after this W07
                w07_index = segments.index(w07_segment)
                next_segments = segments[w07_index+1:]
                
                # Look for G69
                g69_segment = next((s for s in next_segments if s.startswith("G69*")), None)
                if g69_segment:
                    elements = g69_segment.split("*")
                    w07_item["G69"] = elements[1] if len(elements) > 1 else ""
                
                # Find N9 segments that belong to this W07
                next_w07_index = next((i for i, s in enumerate(next_segments) if s.startswith("W07*")), len(next_segments))
                w07_related_segments = next_segments[:next_w07_index]
                
                for segment in w07_related_segments:
                    if segment.startswith("N9*"):
                        elements = segment.split("*")
                        n9_entry = {
                            "referenceIdQualifier": elements[1] if len(elements) > 1 else "",
                            "referenceId": elements[2] if len(elements) > 2 else ""
                        }
                        w07_item["N9"].append(n9_entry)
                
                json_structure["detail"]["W07Loop"].append(w07_item)
        
        # If summary is empty but we have a W13 or W14 segment, add it
        if not json_structure["summary"] and any(s.startswith(("W13*", "W14*")) for s in segments if "*" in s):
            summary_segment = next((s for s in segments if s.startswith(("W13*", "W14*"))), None)
            if summary_segment:
                segment_type = summary_segment.split("*")[0]
                elements = summary_segment.split("*")
                json_structure["summary"][segment_type] = {
                    "totalQuantity": elements[1] if len(elements) > 1 else ""
                }
        
        return json_structure