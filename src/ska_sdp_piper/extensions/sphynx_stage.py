# pragma: exclude file

from inspect import signature

from docutils import nodes
from sphinx.domains.python import PyFunction
from sphinx.ext.autodoc import FunctionDocumenter

from ..framework.configurable_stage import Stage


class StageDocumenter(FunctionDocumenter):
    """Document task definitions."""

    objtype = "stage"
    member_order = 11

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        return isinstance(member, Stage) and getattr(member, "__wrapped__")

    def format_args(self):
        wrapped = getattr(self.object, "__wrapped__", None)
        if wrapped is not None:
            sig = signature(wrapped)
            if "self" in sig.parameters or "cls" in sig.parameters:
                sig = sig.replace(parameters=list(sig.parameters.values())[1:])
            return str(sig)
        return ""

    def document_members(self, all_members=False):
        pass

    def check_module(self):
        wrapped = getattr(self.object, "__wrapped__", None)
        if wrapped and getattr(wrapped, "__module__") == self.modname:
            return True
        return super().check_module()


class StageDirective(PyFunction):
    """Sphinx task directive."""

    def get_signature_prefix(self, sig):
        return [nodes.Text(self.env.config.configurable_stage_prefix)]


def autodoc_skip_member_handler(app, what, name, obj, skip, options):
    """Handler for autodoc-skip-member event."""

    if isinstance(obj, Stage) and getattr(obj, "__wrapped__"):
        if skip:
            return False
    return None


def setup(app):
    """Setup Sphinx extension."""
    app.setup_extension("sphinx.ext.autodoc")
    app.add_autodocumenter(StageDocumenter)
    app.add_directive_to_domain("py", "stage", StageDirective)
    app.add_config_value("configurable_stage_prefix", "(stage)", True)
    app.connect("autodoc-skip-member", autodoc_skip_member_handler)

    return {"parallel_read_safe": True}
