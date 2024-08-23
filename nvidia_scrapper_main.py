import re

from bs4 import BeautifulSoup
from langchain.document_loaders import WebBaseLoader, OnlinePDFLoader
import requests
import pickle
import os

# fixes a bug with asyncio and jupyter
import nest_asyncio


# Function to extract URLs from HTML content
def extract_urls_on_web_page(parent_link, html_content):
    try:
        # Use BeautifulSoup to parse the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')
    except Exception as e:
        print(f"An error occurred while loading batch: {e}")
        return []

    # Example: Extract all 'a' tags (links) from the parsed HTML
    links = soup.find_all('a')

    # Extract href attributes (URLs) from the links
    urls = []
    for link in links:
        if link and link.get('href') and not link.get('href').startswith("/") and '#' not in link.get('href'):

            if link.get('href').startswith("https") and 'index.html' not in link.get('href'):
                urls.append(link.get('href'))
            if 'index.html' in link.get('href'):
                if link.get('href').startswith("https"):
                    urls.append(link.get('href'))
                else:
                    urls.append(parent_link.split('index.html')[0] + link.get('href'))
    return urls


nest_asyncio.apply()

# Specify the filename
filename = 'links.pkl'
processsed_links = []

# Check if the file exists
if os.path.exists(filename):
    with open(filename, 'rb') as f:
        processsed_links = pickle.load(f)
else:
    processsed_links = []


def batch_generator(lst, batch_size):
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]


doc_urls = ['https://docs.nvidia.com/cuda/index.html',
             'https://docs.nvidia.com/jetson/index.html',
             'https://docs.nvidia.com/cloudera/prod_certified-for-cloudera.html',
             'https://docs.nvidia.com/cloud-functions/user-guide/latest/index.html',
             'https://docs.nvidia.com/fleet-command/user-guide/0.1.0/index.html',
             'https://docs.nvidia.com/deploy/index.html',
             'https://developer.nvidia.com/nvcomp',
             'https://docs.nvidia.com/cuda/thrust/index.html',
             'https://docs.nvidia.com/nemo-framework/user-guide/latest/index.html',
             'https://docs.nvidia.com/ai-workbench/user-guide/latest/index.html',
             'https://developer.nvidia.com/rtx/path-tracing/nvapi/get-started',
             'https://docs.nvidia.com/vcr-sdk/index.html',
             'https://docs.nvidia.com/video-technologies/index.html',
             'https://docs.nvidia.com/grid/index.html',
             'https://docs.nvidia.com/ucf/index.html',
             'https://docs.nvidia.com/hpc-sdk/index.html',
             'https://docs.nvidia.com/spark-rapids/user-guide/latest/index.html',
             'https://developer.nvidia.com/blog/accelerating-spark-3-0-and-xgboost-end-to-end-training-and-hyperparameter-tuning/,'
             'https://docs.nvidia.com/deeplearning/transformer-engine/index.html',
             'https://docs.nvidia.com/sdk-manager/index.html',
             'https://docs.nvidia.com/tao/tao-toolkit/index.html',
             'https://docs.nvidia.com/deeplearning/riva/user-guide/docs/index.html',
             'https://raytracing-docs.nvidia.com/index.html',
             'https://catalog.ngc.nvidia.com/orgs/nvidia/teams/magnum-io/containers/magnum-io',
             'https://docs.omniverse.nvidia.com/dev-guide/latest/index.html',
             'https://docs.nvidia.com/datacenter/nvtags/latest/nvtags-user-guide/index.html',
             'https://docs.nvidia.com/nvshmem/api/index.html',
             'https://docs.nvidia.com/nvshmem/release-notes-install-guide/index.html',
             'https://docs.nvidia.com/nsight-aftermath/2023.3/index.html',
             'https://docs.nvidia.com/nsight-visual-studio-edition/index.html',
             'https://docs.nvidia.com/nsight-visual-studio-code-edition/index.html',
             'https://docs.nvidia.com/nsight-systems/index.html',
             'https://docs.nvidia.com/nsight-vs-integration/index.html',
             'https://docs.nvidia.com/nsight-graphics/index.html',
             'https://docs.nvidia.com/cuda/nsight-eclipse-plugins-guide/index.html',
             'https://docs.nvidia.com/nsight-dl-designer/index.html',
             'https://docs.nvidia.com/nsight-compute/index.html',
             'https://docs.nvidia.com/rtx/ngx/programming-guide/index.html',
             'https://docs.nvidia.com/ngc/gpu-cloud/ngc-user-guide/index.html',
             'https://docs.nvidia.com/ngc/gpu-cloud/ngc-catalog-user-guide/index.html',
             'https://docs.nvidia.com/ngc/gpu-cloud/ngc-private-registry-user-guide/index.html',
             'https://docs.ngc.nvidia.com/cli/index.html',
             'https://docs.nvidia.com/morpheus/index.html',
             'https://docs.nvidia.com/modulus/index.html',
             'https://docs.nvidia.com/metropolis/deepstream/dev-guide/',
             'https://nvidia-merlin.github.io/Merlin/main/README.html',
             'https://docs.nvidia.com/deeplearning/maxine/index.html',
             'https://github.com/NVIDIA/libcudacxx/#libcu-the-c-standard-library-for-your-entire-system',
             'https://docs.nvidia.com/isaac/doc/index.html',
             'https://docs.nvidia.com/igx-orin/user-guide/latest/index.html',
             'https://docs.nvidia.com/igx-orin/developer-kit-product-brief/latest/introduction.html',
             'https://github.com/NVIDIAGameWorks/GeForceNOW-SDK',
             'https://docs.nvidia.com/gameworks/index.html',
             'https://github.com/NVIDIA/NVFlare',
             'https://nvflare.readthedocs.io/en/main/index.html',
             'https://developer.download.nvidia.com/designworks/capture-sdk/docs/7.1/NVIDIA-Capture-SDK-FAQ.pdf',
             'https://docs.nvidia.com/cloudxr-sdk/index.html',
             'https://docs.nvidia.com/deeplearning/dali/user-guide/docs/index.html',
             'https://docs.nvidia.com/cuopt/user-guide/overview.html',
             'https://docs.nvidia.com/datacenter/dcgm/latest/user-guide/index.html',
             'https://developer.download.nvidia.com/drive/secure/drive-orin/DRIVE-AGX-Orin-DevKit-QSG-DU-11049-001.pdf?iQHpBV37n1dHSWk4nG8oe38vqNtOTe0cNcKh1SVNPzZcbHmlhtJSooLSH7stAGchbD96H-4jSt7D36fED0I0C3xnSlOsSb6FLS102M-D7ceaLp2Bi0LzJr0PNxlyf8UsdMG2XFDnTEqInEPG3mkr9SFlouXQJ52raSBDEIsDFfnd&t=eyJscyI6ImdzZW8iLCJsc2QiOiJodHRwczovL2NvbGFiLnJlc2VhcmNoLmdvb2dsZS5jb20vIn0=',
             'https://docs.nvidia.com/bionemo-framework/latest/',
             ]
#web_links = ['https://docs.nvidia.com/jetson/index.html']

blog_urls = [
    'https://developer.nvidia.com/blog/recent-posts/',
    'https://developer.nvidia.com/blog/category/generative-ai/',
    'https://developer.nvidia.com/blog/category/simulation-modeling-design/',
    'https://developer.nvidia.com/blog/category/conversational-ai/',
    'https://developer.nvidia.com/blog/category/computer-vision/',
    'https://developer.nvidia.com/blog/category/data-science/',
    'https://developer.nvidia.com/blog/category/data-center-cloud/',
    'https://developer.nvidia.com/blog/category/edge-computing/',
    'https://developer.nvidia.com/blog/category/robotics/',
    '']

forum_urls = [
    'https://forums.developer.nvidia.com/',
    'https://stackoverflow.com/questions/tagged/nvidia?tab=votes',
    'https://stackoverflow.com/questions/tagged/cuda?tab=votes',
    'https://stackoverflow.com/questions/tagged/nvidia-jetson?tab=votes',
    'https://stackoverflow.com/questions/tagged/nvidia-jetson-nano?tab=votes',
    'https://stackoverflow.com/questions/tagged/nvidia-docker?tab=Votes',
    'https://stackoverflow.com/questions/tagged/nvidia-digits?tab=Votes',
    'https://stackoverflow.com/questions/tagged/nvidia-deepstream?tab=Votes',
    'https://stackoverflow.com/questions/tagged/nvidia-smi?tab=Votes',
    'https://stackoverflow.com/questions/tagged/nvidia-titan?tab=Votes',
    'https://stackoverflow.com/questions/tagged/nvidia-flex?tab=Votes',
    'https://stackoverflow.com/questions/tagged/nvidia-isaac?tab=Votes',
    'https://stackoverflow.com/questions/tagged/nvidia-hpc-compilers?tab=Votes',
    'https://stackoverflow.com/questions/tagged/nvidia-jetpack-sdk?tab=Votes',
    'https://stackoverflow.com/questions/tagged/nvidia-sass?tab=Votes'
]


web_links_list = forum_urls

for web_link in web_links_list:
    print(f"Processing {web_link}")
    web_links = [web_link]

    depth = 4
    count = 0
    # Define your batch size
    batch_size = 50  # Adjust the batch size as needed


    # List to hold PDF links
    pdf_links = []
    pdf_link_pattern = re.compile(r'https?://.*\.pdf$', re.IGNORECASE)


    # Initialize an empty list to store the loaded documents
    documents = []
    docstore_data_path = "documents.pkl"
    # Specify the filename
    link_path = 'links.pkl'


    loader = WebBaseLoader(web_links)
    try:
        loader.requests_per_second = 2
        docs = loader.aload()
        documents += docs
        processsed_links += web_links
    except Exception as e:
        print(f"An error occurred while loading batch: {e}")

    while count < depth:
        new_links = set()

        print(f"web_links to process {len(web_links)}")
        print(f"Processing {web_links}")
        # Process the URLs in batches
        for batch in batch_generator(web_links, batch_size):
            print(f"batch - {batch}")
            for link in batch[:]:  # Iterate over a slice copy of the list
                if pdf_link_pattern.match(link):
                    pdf_links.append(link)
                    batch.remove(link)
            loader = WebBaseLoader(batch)
            processsed_links += batch
            try:
                loader.requests_per_second = 2
                docs = loader.aload()
                documents += docs
                print(f"Document count - {len(documents)}")
            except Exception as e:
                print(f"An error occurred while loading batch: {e}")

            if len(pdf_links) > 0:
                for pdf_link in pdf_links:
                    try:
                        loader = OnlinePDFLoader(pdf_link)
                        processsed_links.append(pdf_link)
                        # Load the document by calling loader.load()
                        docs = loader.load()
                        documents += docs
                        print(f"Document count - {len(documents)}")
                    except Exception as e:
                        print(f"An error occurred while loading batch: {e}")

        # Process each loaded document with BeautifulSoup
        for link in web_links:
            print(link)
            try:
                html_doc = requests.get(link, timeout=10)
            except Exception as e:
                print(f"An error occurred while loading batch: {e}")
                continue
            # Extract URLs from the HTML content
            links = extract_urls_on_web_page(link, html_doc.text)
            web_links.remove(link)
            if len(links) > 0:
                for link in links:
                    # forums
                    if ((("nvidia" in link and "forums" in link) and "https" in link)
                            or ("nvidia" in link and "stackoverflow" in link) and "https" in link):
                    # blogs
                    #if ("nvidia" in link and "blog" in link) and "https" in link:
                    # sdk docs
                    # if ("nvidia" in link or "nvflare" in link) and "https" in link:
                        new_links.add(link)

        web_links = [x for x in new_links if x not in processsed_links]
        if len(web_links) == 0:
            break
        count += 1
        print(f"count {count}")

        print("Dumping in pickle jar!!")
        temp_doc = []
        if os.path.exists(docstore_data_path):
            with open(docstore_data_path, 'rb') as f:
                temp_doc = pickle.load(f)

        temp_doc += documents
        # Write the updated data back to the pickle file
        with open(docstore_data_path, 'wb') as file:
            pickle.dump(temp_doc, file)
        documents = []


    print("Dumping links!!")
    # Write the updated data back to the pickle file
    temp_links = []
    if os.path.exists(link_path):
        with open(link_path, 'rb') as f:
            temp_links = pickle.load(f)

    temp_links += processsed_links
    # Write the updated data back to the pickle file
    with open(link_path, 'wb') as file:
        pickle.dump(temp_links, file)

    print(f"web_links {processsed_links}")