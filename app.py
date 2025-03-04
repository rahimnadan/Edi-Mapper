
import streamlit as st
import json
import os
import uuid
from datetime import datetime
import logging
import configparser
import time

from edi_parser import EDIParser
from ftp_transfer import FTPTransfer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("EDIMapper")

# Load configuration
def load_config():
    """Load configuration from config file or environment variables"""
    # First try to load from .env file
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # .env file loading is optional
    
    config = {
        "openai_api_key": os.environ.get("OPENAI_API_KEY", ""),
        "ftp": {
            "host": os.environ.get("FTP_HOST", ""),
            "port": os.environ.get("FTP_PORT", "21"),
            "username": os.environ.get("FTP_USER", ""),
            "password": os.environ.get("FTP_PASS", ""),
            "path": os.environ.get("FTP_PATH", "/"),
            "use_sftp": os.environ.get("USE_SFTP", "false").lower() == "true",
            "use_ftps": os.environ.get("USE_FTPS", "false").lower() == "true",
            "timeout": os.environ.get("FTP_TIMEOUT", "30")
        }
    }
    
    # Try to load from config file if it exists
    if os.path.exists("config.ini"):
        conf = configparser.ConfigParser()
        conf.read("config.ini")
        
        if "OpenAI" in conf:
            config["openai_api_key"] = conf["OpenAI"].get("api_key", config["openai_api_key"])
        
        if "FTP" in conf:
            ftp_section = conf["FTP"]
            config["ftp"]["host"] = ftp_section.get("host", config["ftp"]["host"])
            config["ftp"]["port"] = ftp_section.get("port", config["ftp"]["port"])
            config["ftp"]["username"] = ftp_section.get("username", config["ftp"]["username"])
            config["ftp"]["password"] = ftp_section.get("password", config["ftp"]["password"])
            config["ftp"]["path"] = ftp_section.get("path", config["ftp"]["path"])
            config["ftp"]["use_sftp"] = ftp_section.get("use_sftp", "false").lower() == "true"
            config["ftp"]["use_ftps"] = ftp_section.get("use_ftps", "false").lower() == "true"
            config["ftp"]["timeout"] = ftp_section.get("timeout", config["ftp"]["timeout"])
    
    return config

def save_config(config):
    """Save configuration to config file"""
    conf = configparser.ConfigParser()
    
    conf["OpenAI"] = {
        "api_key": config["openai_api_key"]
    }
    
    conf["FTP"] = {
        "host": config["ftp"]["host"],
        "port": config["ftp"]["port"],
        "username": config["ftp"]["username"],
        "password": config["ftp"]["password"],
        "path": config["ftp"]["path"],
        "use_sftp": str(config["ftp"]["use_sftp"]).lower(),
        "use_ftps": str(config["ftp"]["use_ftps"]).lower(),
        "timeout": config["ftp"]["timeout"]
    }
    
    with open("config.ini", "w") as f:
        conf.write(f)

# Process EDI data
def process_edi_data(edi_data, config, use_direct_parser=False):
    """Process EDI data and convert to JSON using LLM or direct parser
    
    Args:
        edi_data: The raw EDI data to process
        config: Configuration dictionary
        use_direct_parser: Force use of direct parser instead of LLM
    
    Returns:
        tuple: (success, result, message)
    """
    try:
        # Clean up the EDI data by removing any extra whitespace
        cleaned_edi_data = edi_data.strip()
        
        # Basic validation to ensure it looks like EDI data
        if "~" not in cleaned_edi_data or "*" not in cleaned_edi_data:
            return False, None, "Invalid EDI data format. The data should contain both ~ and * delimiters."
        
        # Check for required segments
        required_segments = ["W17", "N1"]
        missing_segments = [seg for seg in required_segments if seg + "*" not in cleaned_edi_data]
        
        if missing_segments:
            missing_str = ", ".join(missing_segments)
            return False, None, f"EDI data is missing required segments: {missing_str}"
        
        # Initialize parser
        parser = EDIParser(api_key=config["openai_api_key"])
        
        # Use direct parser if requested or if no API key
        if use_direct_parser or not config["openai_api_key"]:
            logger.warning("Using direct parser instead of LLM parser")
            start_time = time.time()
            result = parser._direct_parser(cleaned_edi_data)
            processing_time = time.time() - start_time
            logger.info(f"Processed EDI data with direct parser in {processing_time:.2f} seconds")
            return True, result, f"EDI data processed with direct parser in {processing_time:.2f} seconds"
        
        # Use LLM parser
        logger.info("Processing EDI data with LLM parser")
        start_time = time.time()
        result = parser.parse(cleaned_edi_data)
        processing_time = time.time() - start_time
        
        # Validate result
        if not result or not isinstance(result, dict):
            logger.error(f"Invalid result from parser: {result}")
            return False, None, "EDI parser returned invalid result"
        
        # Check for required fields in the result
        if "header" not in result or "detail" not in result or "summary" not in result:
            logger.error(f"Missing required sections in result: {result.keys()}")
            return False, None, "Parsed result is missing required sections (header, detail, or summary)"
        
        logger.info(f"Processed EDI data in {processing_time:.2f} seconds")
        return True, result, f"EDI data processed successfully in {processing_time:.2f} seconds"
    
    except Exception as e:
        logger.error(f"Error processing EDI data: {str(e)}", exc_info=True)
        
        # Try direct parser as fallback
        try:
            logger.info("Attempting fallback to direct parser")
            parser = EDIParser(api_key="")  # Empty API key forces direct parser
            result = parser._direct_parser(edi_data)
            return True, result, "EDI data processed with fallback direct parser"
        except Exception as fallback_e:
            logger.error(f"Fallback parser also failed: {str(fallback_e)}", exc_info=True)
            return False, None, f"Error processing EDI data: {str(e)}. Fallback parser also failed."

# Transfer JSON data to FTP
def transfer_to_ftp(json_data, filename, config):
    """Transfer JSON data to FTP server"""
    try:
        ftp = FTPTransfer(config["ftp"])
        start_time = time.time()
        success, message = ftp.transfer(json_data, filename)
        transfer_time = time.time() - start_time
        
        if success:
            logger.info(f"Transferred data to FTP in {transfer_time:.2f} seconds")
            return True, f"{message} in {transfer_time:.2f} seconds"
        else:
            logger.error(f"FTP transfer failed: {message}")
            return False, message
    except Exception as e:
        logger.error(f"Error in FTP transfer: {str(e)}")
        return False, f"Error in FTP transfer: {str(e)}"

# Streamlit UI
def main():
    st.set_page_config(
        page_title="EDI 944 Mapper",
        page_icon="📄",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Load configuration
    if "config" not in st.session_state:
        st.session_state.config = load_config()
    
    # Initialize session state
    if "json_result" not in st.session_state:
        st.session_state.json_result = None
    
    if "processing_status" not in st.session_state:
        st.session_state.processing_status = None
    
    # Application title and description
    st.title("EDI 944 to JSON Mapper with FTP Transfer")
    st.markdown("""
    This application converts EDI 944 (Warehouse Stock Transfer Receipt) data to a standardized JSON format
    and transfers it to an FTP server. The mapping is powered by an LLM using advanced ReAct prompting. To get started, please enter the Configuration details(OpenAI API Key) and click Save Configuration.
    """)
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        
        # API Key configuration
        st.subheader("OpenAI API Key")
        api_key = st.text_input(
            "API Key",
            value=st.session_state.config["openai_api_key"],
            type="password",
            key="openai_api_key_input"
        )
        
        # Immediately update the config when API key is changed
        if api_key != st.session_state.config["openai_api_key"]:
            st.session_state.config["openai_api_key"] = api_key
            # Also update session state for immediate use
            save_config(st.session_state.config)
            st.success("API Key updated!")
        
        # FTP configuration
        st.subheader("FTP/SFTP Connection")
        ftp_host = st.text_input("Host", value=st.session_state.config["ftp"]["host"])
        ftp_port = st.text_input("Port", value=st.session_state.config["ftp"]["port"])
        ftp_user = st.text_input("Username", value=st.session_state.config["ftp"]["username"])
        ftp_pass = st.text_input("Password", value=st.session_state.config["ftp"]["password"], type="password")
        ftp_path = st.text_input("Path", value=st.session_state.config["ftp"]["path"])
        
        conn_type = st.radio(
            "Connection Type",
            options=["FTP", "FTPS", "SFTP"],
            index=2 if st.session_state.config["ftp"]["use_sftp"] else 1 if st.session_state.config["ftp"]["use_ftps"] else 0
        )
        
        use_sftp = conn_type == "SFTP"
        use_ftps = conn_type == "FTPS"
        
        # Save configuration
        if st.button("Save Configuration"):
            st.session_state.config["openai_api_key"] = api_key
            st.session_state.config["ftp"]["host"] = ftp_host
            st.session_state.config["ftp"]["port"] = ftp_port
            st.session_state.config["ftp"]["username"] = ftp_user
            st.session_state.config["ftp"]["password"] = ftp_pass
            st.session_state.config["ftp"]["path"] = ftp_path
            st.session_state.config["ftp"]["use_sftp"] = use_sftp
            st.session_state.config["ftp"]["use_ftps"] = use_ftps
            
            save_config(st.session_state.config)
            st.success("Configuration saved!")
    
    # Main area with tabs
    tab1, tab2, tab3 = st.tabs(["EDI Mapping", "Results", "Logs"])
    
    # EDI Mapping tab
    with tab1:
        st.header("EDI 944 Data Input")
        
        # Sample data button
        if st.button("Load Sample EDI 944 Data"):
            sample_edi = """ISA*00*          *00*          *ZZ*DCG            *ZZ*9083514477     *220519*0800*U*00401*000001057*1*P*>~GS*RE*DCG*9083514477*20220519*0800*1057*X*004010~ST*944*0001~W17*F*20220516*EISU9397985-21104*21104*EISU9397985*9*1337~N1*WH*D7~N9*ZZ*EISU9397985~N9*IN*0100-128E EGLV11020001328~W07*3024*EA*196272171026*VN*HCZK203-STK~G69*3PC LIFE WITH MAMMALS SHORT SET~N9*CL*GREY~N9*SZ*PPK~N9*PO*CS22/0406~N9*LN*18.000~N9*WD*13.000~N9*HT*19.000~N9*WT*24.200~W07*6000*EA*196272482689*VN*HCZK403-STK~G69*3PC LIFE WITH MAMMALS SHORT SET~N9*CL*GREY~N9*SZ*PPK~N9*PO*CS22/0406~N9*LN*18.000~N9*WD*13.000~N9*HT*19.000~N9*WT*27.940~W14*31248~SE*70*0001~GE*1*1057~IEA*1*000001057~"""
            st.session_state.edi_data = sample_edi
        
        # Check if we should clear the text area based on our flag
        if 'clear_data_flag' in st.session_state and st.session_state['clear_data_flag']:
            # Clear the flag
            st.session_state['clear_data_flag'] = False
            # Pre-set the edi_data to empty string before creating the widget
            if 'edi_data' in st.session_state:
                st.session_state['edi_data'] = ""
        
        # Input area for EDI data
        edi_data = st.text_area(
            "Paste EDI 944 data here:",
            height=300,
            key="edi_data"
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Process EDI button
            if st.button("Process EDI Data", type="primary"):
                if not edi_data:
                    st.error("Please enter EDI data to process")
                else:
                    # Check for API key
                    api_key = st.session_state.config["openai_api_key"]
                    
                    # If API key is not in config but was entered in sidebar, use that
                    if not api_key and "openai_api_key_input" in st.session_state:
                        api_key = st.session_state.openai_api_key_input
                        # Update config
                        st.session_state.config["openai_api_key"] = api_key
                    
                    if not api_key:
                        # We'll proceed with a warning that we're using direct parser
                        st.warning("No OpenAI API key provided. Will use direct parser instead of LLM.")
                        use_direct_parser = True
                    else:
                        use_direct_parser = False
                    
                    with st.spinner("Processing EDI data..."):
                        # Pass the use_direct_parser flag to process_edi_data
                        success, result, message = process_edi_data(
                            edi_data, 
                            st.session_state.config,
                            use_direct_parser
                        )
                        
                        if success:
                            st.session_state.json_result = result
                            st.session_state.processing_status = {"success": True, "message": message}
                            st.success(message)
                        else:
                            st.session_state.processing_status = {"success": False, "message": message}
                            st.error(message)
        
        # Clear button
        with col2:
            if st.button("Clear Data"):
                # Use Streamlit's rerun mechanism instead of directly modifying session state
                # This will clear the form on the next run
                for key in ['json_result', 'processing_status']:
                    if key in st.session_state:
                        del st.session_state[key]
                
                # To clear the text area, we need a special approach
                # Set a flag that we'll check when creating the text area
                st.session_state['clear_data_flag'] = True
                st.rerun()  # Use st.rerun() instead of st.experimental_rerun()
    
    # Results tab
    with tab2:
        st.header("JSON Result")
        
        if st.session_state.json_result:
            # Display JSON result
            st.json(st.session_state.json_result)
            
            # Generate a default filename
            default_filename = f"EDI944_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Transfer to FTP section
            st.subheader("Transfer to FTP")
            
            col1, col2 = st.columns(2)
            
            with col1:
                filename = st.text_input("Filename (without extension):", value=default_filename)
            
            with col2:
                # Transfer to FTP button
                if st.button("Transfer to FTP", type="primary"):
                    if not st.session_state.config["ftp"]["host"]:
                        st.error("Please configure FTP settings in the sidebar")
                    else:
                        with st.spinner("Transferring to FTP..."):
                            success, message = transfer_to_ftp(
                                st.session_state.json_result,
                                filename,
                                st.session_state.config
                            )
                            
                            if success:
                                st.success(message)
                            else:
                                st.error(message)
            
            # Download JSON button
            if st.download_button(
                label="Download JSON",
                data=json.dumps(st.session_state.json_result, indent=2),
                file_name=f"{filename}.json",
                mime="application/json"
            ):
                st.info("JSON file downloaded successfully")
        else:
            st.info("Process EDI data to see results here")
    
    # Logs tab
    with tab3:
        st.header("Application Logs")
        
        if os.path.exists("app.log"):
            with open("app.log", "r") as log_file:
                log_content = log_file.read()
                st.text_area("Logs", value=log_content, height=400, disabled=True)
        else:
            st.info("No logs available yet")

if __name__ == "__main__":
    main()