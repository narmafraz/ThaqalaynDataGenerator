from bs4 import BeautifulSoup, NavigableString, Tag


def is_rtl_tag(element: Tag) -> bool:
	return element.has_attr('dir') and element['dir'] == 'rtl'

def is_tag(element) -> bool:
    return isinstance(element, Tag)

def get_contents(element):
	return "".join([str(x) for x in element.contents])
