"""Validate generated HTML without executing or rewriting its contents."""

from html.parser import HTMLParser


URL_ATTRIBUTES = frozenset({"href", "src", "xlink:href", "action", "formaction", "data", "poster", "background", "cite"})


class _StaticHTML(HTMLParser):
    def handle_starttag(self, tag, attributes):
        if tag == "script":
            raise ValueError("Public HTML contains a browser script")
        for name, value in attributes:
            if name.startswith("on"):
                raise ValueError("Public HTML contains a browser event handler")
            if name in URL_ATTRIBUTES and value is not None and "".join(value.split()).lower().startswith("javascript:"):
                raise ValueError("Public HTML contains a JavaScript URL")


def validate_static_html(path):
    parser = _StaticHTML(convert_charrefs=True)
    with path.open(encoding="utf-8") as stream:
        while chunk := stream.read(65536):
            parser.feed(chunk)
    parser.close()
