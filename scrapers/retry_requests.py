import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def requests_retry_session(url, retries=3, backoff_factor=0.3,
                           status_forcelist=(500, 502, 504),
                           session=None):
    ''' Define request retries attempts.

    Args:
        url: url to get
        retries: total number of retry attempts
        backoff_factor: amount of time between attempts
        status_forcelist: retry if response is in list
        session: requests session object

    Example:
        req = requests_retry_session().get(<url>)
        print(req.status_code)

    Info:
        https://www.peterbe.com/plog/best-practice-with-retries-with-requests
    '''
    session = session or requests.Session()
    retry = Retry(total=retries,
                  read=retries,
                  connect=retries,
                  backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session.get(url)
