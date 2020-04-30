import os
import os.path
import sys
import re
import tempfile
import inspect
from importlib import import_module
from threading import Thread
from importlib import reload  # requires >= py 3.4

from win32com.client import Dispatch, CDispatch
import pywintypes

from . import conversion, xlplatform, Range, apps
from .utils import VBAWriter
import pandas as pd
import warnings

cache = {}
cache_count_references = {}
cache_to_delete = {}
cache_reapeated_formulas = {}
first_formula_func = re.compile(r"(.*?)\(")


def get_first_function(formula):
    res = first_formula_func.match(formula)
    if res is None:
        return
    return res.group(0).replace("=","").replace("(","").strip()


class AsyncThread(Thread):
    def __init__(self, pid, book_name, sheet_name, address, func, args, cache_key, expand):
        Thread.__init__(self)
        self.pid = pid
        self.book = book_name
        self.sheet = sheet_name
        self.address = address
        self.func = func
        self.args = args
        self.cache_key = cache_key
        self.expand = expand

    def run(self):
        #apps[self.pid].books[self.book].sheets[self.sheet][self.address].api._inner
        new_caller = None
        for i, arg in enumerate(self.args):
            if isinstance(arg, CDispatch):
                if new_caller is None:
                    # FIXME this should simulate the actual object in main thread
                    r"""
                     File "H:\common_partition\MEGAsync\python_projects\usb_projects\usb_python_env\lib\site-packages\xlwings\_xlwindows.py", line 602, in range
                        xl1 = self.xl.Range(arg1)
                    AttributeError: 'NoneType' object has no attribute 'Range'
                    """
                    new_caller = apps[self.pid].books[self.book].sheets[self.sheet][self.address]#.api._inner
                self.args[i] = new_caller  # replace stale caller with new
        cache[self.cache_key] = self.func(*self.args)

        if self.expand:
            apps[self.pid].books[self.book].sheets[self.sheet][self.address].formula_array = \
                apps[self.pid].books[self.book].sheets[self.sheet][self.address].formula_array
        else:
            apps[self.pid].books[self.book].sheets[self.sheet][self.address].formula = \
                apps[self.pid].books[self.book].sheets[self.sheet][self.address].formula


def func_sig(f):
    s = inspect.signature(f)
    vararg = None
    args = []
    defaults = []
    for p in s.parameters.values():
        if p.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD:
            args.append(p.name)
            if p.default is not inspect.Signature.empty:
                defaults.append(p.default)
        elif p.kind is inspect.Parameter.VAR_POSITIONAL:
            args.append(p.name)
            vararg = p.name
        else:
            raise Exception("xlwings does not support UDFs with keyword arguments")
    return {
        'args': args,
        'defaults': defaults,
        'vararg': vararg
    }


def get_category(**func_kwargs):
    if 'category' in func_kwargs:
        category = func_kwargs.pop('category')
        if isinstance(category, int):
            if 1 <= category <= 14:
                return category
            raise Exception(
                'There is only 14 build-in categories available in Excel. Please use a string value to specify a custom category.')
        if isinstance(category, str):
            return category[:255]
        raise Exception(
            'Category {0} should either be a predefined Excel category (int value) or a custom one (str value).'.format(
                category))
    return "xlwings"  # Default category


def get_async_mode(**func_kwargs):
    if 'async_mode' in func_kwargs:
        value = func_kwargs.pop('async_mode')
        if value in ['threading']:
            return value
        raise Exception('The only supported async_mode mode is currently "threading".')
    else:
        return None


def check_bool(kw, **func_kwargs):
    if kw in func_kwargs:
        check = func_kwargs.pop(kw)
        if isinstance(check, bool):
            return check
        raise Exception('{0} only takes boolean values. ("{1}" provided).'.format(kw, check))
    return False


def xlfunc(f=None, **kwargs):
    def inner(f):
        if not hasattr(f, "__xlfunc__"):
            xlf = f.__xlfunc__ = {}
            xlf["name"] = f.__name__
            xlf["sub"] = False
            xlargs = xlf["args"] = []
            xlargmap = xlf["argmap"] = {}
            sig = func_sig(f)
            nArgs = len(sig['args'])
            nDefaults = len(sig['defaults'])
            nRequiredArgs = nArgs - nDefaults
            if sig['vararg'] and nDefaults > 0:
                raise Exception("xlwings does not support UDFs with both optional and variable length arguments")
            for vpos, vname in enumerate(sig['args']):
                arg_info = {
                    "name": vname,
                    "pos": vpos,
                    "vba": None,
                    "doc": "Positional argument " + str(vpos + 1),
                    "vararg": vname == sig['vararg'],
                    "options": {}
                }
                if vpos >= nRequiredArgs:
                    arg_info["optional"] = sig['defaults'][vpos - nRequiredArgs]
                xlargs.append(arg_info)
                xlargmap[vname] = xlargs[-1]
            xlf["ret"] = {
                "doc": f.__doc__ if f.__doc__ is not None else "Python function '" + f.__name__ + "' defined in '" + str(f.__code__.co_filename) + "'.",
                "options": {}
            }
        f.__xlfunc__["category"] = get_category(**kwargs)
        f.__xlfunc__['call_in_wizard'] = check_bool('call_in_wizard', **kwargs)
        f.__xlfunc__['volatile'] = check_bool('volatile', **kwargs)
        f.__xlfunc__['async_mode'] = get_async_mode(**kwargs)
        return f
    if f is None:
        return inner
    else:
        return inner(f)


def xlsub(f=None, **kwargs):
    def inner(f):
        f = xlfunc(**kwargs)(f)
        f.__xlfunc__["sub"] = True
        return f
    if f is None:
        return inner
    else:
        return inner(f)


def xlret(convert=None, **kwargs):
    if convert is not None:
        kwargs['convert'] = convert
    def inner(f):
        xlf = xlfunc(f).__xlfunc__
        xlr = xlf["ret"]
        xlr['options'].update(kwargs)
        return f
    return inner


def xlarg(arg, convert=None, **kwargs):
    if convert is not None:
        kwargs['convert'] = convert
    def inner(f):
        xlf = xlfunc(f).__xlfunc__
        if arg not in xlf["argmap"]:
            raise Exception("Invalid argument name '" + arg + "'.")
        xla = xlf["argmap"][arg]
        for special in ('vba', 'doc'):
            if special in kwargs:
                xla[special] = kwargs.pop(special)
        xla['options'].update(kwargs)
        return f
    return inner


udf_modules = {}


class DelayedResizeDynamicArrayFormula:
    def __init__(self, target_range, caller, needs_clearing):
        self.target_range = target_range
        self.caller = caller
        self.needs_clearing = needs_clearing

    def __call__(self, *args, **kwargs):
        formula = self.caller.FormulaArray
        if self.needs_clearing:
            self.caller.ClearContents()
        self.target_range.api.FormulaArray = formula


def get_udf_module(module_name):
    module_info = udf_modules.get(module_name, None)
    if module_info is not None:
        module = module_info['module']
        # If filetime is None, it's not reloadable
        if module_info['filetime'] is not None:
            mtime = os.path.getmtime(module_info['filename'])
            if mtime != module_info['filetime']:
                module = reload(module)
                module_info['filetime'] = mtime
                module_info['module'] = module
    else:
        if sys.version_info[:2] < (2, 7):
            # For Python 2.6. we don't handle modules in subpackages
            module = __import__(module_name)
        else:
            module = import_module(module_name)

        filename = os.path.normcase(module.__file__.lower())

        try:  # getmtime fails for zip imports and frozen modules
            mtime = os.path.getmtime(filename)
        except OSError:
            mtime = None

        udf_modules[module_name] = {
            'filename': filename,
            'filetime': mtime,
            'module': module
        }

    return module


def get_cache_key(func, args, caller):
    """only use this if function is called from cells, not VBA"""
    xw_caller = Range(impl=xlplatform.Range(xl=caller))
    # FIXED bug in which pandas dataframes were converted to test for the key but only some values appeared in the
    # cache key so when values from source were updated key didn't change getting outdated cached data
    # DONE improved key for any cell so that cache can be for multiple cells
    return (func.__name__, str([i.to_string() if isinstance(i, pd.DataFrame) else i for i in args]))
    #return (func.__name__, str([i.to_string() if isinstance(i, DataFrame) else i for i in args]),
    #        str(xw_caller.sheet.book.app.pid) + xw_caller.sheet.book.name + xw_caller.sheet.name + xw_caller.address.split(':')[0])


def _get_str(data):
    if isinstance(data, str):
        return data
    else:
        return _get_str(data[0])

def call_udf(module_name, func_name, args, this_workbook=None, caller=None):

    module = get_udf_module(module_name)
    func = getattr(module, func_name)
    func_info = func.__xlfunc__
    args_info = func_info['args']
    ret_info = func_info['ret']
    is_dynamic_array = ret_info['options'].get('expand')

    writing = func_info.get('writing', None)
    if writing and writing == caller.Address:
        return func_info['rval']

    output_param_indices = []

    args = list(args)
    for i, arg in enumerate(args):
        arg_info = args_info[min(i, len(args_info)-1)]
        if type(arg) is int and arg == -2147352572:      # missing
            args[i] = arg_info.get('optional', None)
        elif xlplatform.is_range_instance(arg):
            if arg_info.get('output', False):
                output_param_indices.append(i)
                args[i] = OutputParameter(Range(impl=xlplatform.Range(xl=arg)), arg_info['options'], func, caller)
            else:
                args[i] = conversion.read(Range(impl=xlplatform.Range(xl=arg)), None, arg_info['options'])
        else:
            args[i] = conversion.read(None, arg, arg_info['options'])
    if this_workbook:
        xlplatform.BOOK_CALLER = Dispatch(this_workbook)

    # get actual caller range
    xw_caller = Range(impl=xlplatform.Range(xl=caller))
    eq_range = F"'[{xw_caller.sheet.book.name}]{xw_caller.sheet.name}'!{xw_caller.address}"
    eq_start = eq_range.split(":")[0]
    alternative_eq = xw_caller.sheet[eq_start].formula.replace(",",";") # FIX BUG # _get_str(xw_caller.formula)
    this_equation = alternative_eq   # BUGFIX to not getting equation
    #try:
    #    this_equation = caller.FormulaArray  # actual formula
    #    if alternative_eq != this_equation:
    #        warnings.warn(f"alternative equation '{alternative_eq}' differente to real '{this_equation}'")
    #except pywintypes.com_error:
    #    if alternative_eq:
    #        warnings.warn(f"could not retrieve equation on addres {eq_start}. alternative '{alternative_eq}''")
    #        this_equation = alternative_eq
    #    else:
    #        print(f"could not retrieve equation on addres {eq_start}")
    #        raise

    # format: func_name, args
    cache_key = get_cache_key(func, args, caller)
    last_func_count = this_equation.count(func_name)
    eq_key = (eq_start, this_equation)  # creates key with start of equation address and equation

    # get the counts of last function in the equation to prevent prints to excel until the last opperation
    is_last_function = func_name == get_first_function(this_equation)
    if is_last_function:
        if last_func_count > 1:
            if eq_key in cache_reapeated_formulas:
                # reduce real count usage
                cache_reapeated_formulas[eq_key] -= 1
                last_func_count = cache_reapeated_formulas[eq_key]
            else:
                # start to track function execution
                cache_reapeated_formulas[eq_key] = last_func_count
            print(F"executing iteration {last_func_count} in function '{func_name}' result of {eq_key}")
        if last_func_count <= 1:
            try:
                del cache_reapeated_formulas[eq_key]
            except KeyError:
                pass

    save_new_cache = False
    try:
        stored_eq, stored_keys = cache_to_delete[eq_start]

        # check if equation changed
        if stored_eq != this_equation:
            # process of eliminating cache of old equation
            for stored_cache in stored_keys:

                # check if there are no more referencese on old cache, it should be deleted
                count_ref = cache_count_references.get(stored_cache, None)
                if count_ref is not None:
                    count_ref -= 1

                # delete cache if there are no more references
                if count_ref is not None and count_ref <= 0:
                    if stored_cache == cache_key:
                        continue   # do not dete cache to current reference

                    # delete cache
                    print(f"deleting cache of eq '{stored_eq}' in cell {eq_start}")
                    for i, my_cache in enumerate((cache, cache_count_references)):
                        try:
                            # prevent memory leakage
                            # traying to delete old data
                            del my_cache[stored_cache]
                        except KeyError:
                            pass
                else:
                    # update decrease
                    cache_count_references[stored_cache] = count_ref
            save_new_cache = True
        else:
            # check if this is a new cache to keep track of it for deleting later
            if cache_key not in stored_keys:
                # same equation, new key to keep reference
                count = cache_count_references.get(cache_key, 0)
                cache_count_references[cache_key] = count + 1

                # add key to tracked cache_key
                stored_keys.add(cache_key)

    except KeyError:
        save_new_cache = True

    if save_new_cache:
        # increase number of references as cache is used in this cell equation
        count = cache_count_references.get(cache_key, 0)
        cache_count_references[cache_key] = count + 1

        # store new information of caches in equation cell
        store_keys = {cache_key}
        cache_to_delete[eq_start] = (this_equation, store_keys)

    if func_info['async_mode'] and func_info['async_mode'] == 'threading':
        cached_value = cache.get(cache_key)
        if cached_value is not None:  # test against None as np arrays don't have a truth value
            if not is_dynamic_array:  # for dynamic arrays, the cache is cleared below
                del cache[cache_key]
            ret = cached_value
        else:
            # You can't pass pywin32 objects directly to threads
            thread = AsyncThread(xw_caller.sheet.book.app.pid,
                                 xw_caller.sheet.book.name,
                                 xw_caller.sheet.name,
                                 xw_caller.address,
                                 func,
                                 args,
                                 cache_key,
                                 is_dynamic_array)
            thread.start()
            return [["#N/A waiting..." * xw_caller.columns.count] * xw_caller.rows.count]
    else:
        if is_dynamic_array:
            cached_value = cache.get(cache_key)
            if cached_value is not None:
                ret = cached_value
            else:
                ret = func(*args)
                cache[cache_key] = ret
        else:
            ret = func(*args)

    xl_result = conversion.write(ret, None, ret_info['options'])

    if is_dynamic_array:
        current_size = (caller.Rows.Count, caller.Columns.Count)
        result_size = (1, 1)
        if type(xl_result) is list:
            result_height = len(xl_result)
            result_width = result_height and len(xl_result[0])
            result_size = (max(1, result_height), max(1, result_width))

        if current_size != result_size:  # this should be done at the end of the execution of the equation as resizing can throw error if it keeps resizing at every nested equation evaluation. Only print to excel at the end
            # Only write to excel if this is last result
            if is_last_function and last_func_count <=1:
                from .server import add_idle_task
                print(F"executed with resize at iteration {last_func_count} in function '{func_name}' result of {eq_key}")
                
                add_idle_task(DelayedResizeDynamicArrayFormula(
                    Range(impl=xlplatform.Range(xl=caller)).resize(*result_size),
                    caller,
                    current_size[0] > result_size[0] or current_size[1] > result_size[1]
                ))
            else:
                print(f"needs clearing! but ignored. iteration {last_func_count} in function '{func_name}' result of {eq_key}")

    return xl_result


def generate_vba_wrapper(module_name, module, f):

    vba = VBAWriter(f)

    for svar in map(lambda attr: getattr(module, attr), dir(module)):
        if hasattr(svar, '__xlfunc__'):
            xlfunc = svar.__xlfunc__
            xlret = xlfunc['ret']
            fname = xlfunc['name']
            call_in_wizard = xlfunc['call_in_wizard']
            volatile = xlfunc['volatile']

            ftype = 'Sub' if xlfunc['sub'] else 'Function'

            func_sig = ftype + " " + fname + "("

            first = True
            vararg = ''
            n_args = len(xlfunc['args'])
            for arg in xlfunc['args']:
                if not arg['vba']:
                    argname = arg['name']
                    if not first:
                        func_sig += ', '
                    if 'optional' in arg:
                        func_sig += 'Optional '
                    elif arg['vararg']:
                        func_sig += 'ParamArray '
                        vararg = argname
                    func_sig += argname
                    if arg['vararg']:
                        func_sig += '()'
                    first = False
            func_sig += ')'

            with vba.block(func_sig):

                if ftype == 'Function':
                    if not call_in_wizard:
                        vba.writeln('If (Not Application.CommandBars("Standard").Controls(1).Enabled) Then Exit Function')
                    if volatile:
                        vba.writeln('Application.Volatile')

                if vararg != '':
                    vba.writeln("Dim argsArray() As Variant")
                    non_varargs = [arg['vba'] or arg['name'] for arg in xlfunc['args'] if not arg['vararg']]
                    vba.writeln("argsArray = Array(%s)" % tuple({', '.join(non_varargs)}))

                    vba.writeln("ReDim Preserve argsArray(0 to UBound(" + vararg + ") - LBound(" + vararg + ") + " + str(len(non_varargs)) + ")")
                    vba.writeln("For k = LBound(" + vararg + ") To UBound(" + vararg + ")")
                    vba.writeln("argsArray(" + str(len(non_varargs)) + " + k - LBound(" + vararg + ")) = " + argname + "(k)")
                    vba.writeln("Next k")

                    args_vba = 'argsArray'
                else:
                    args_vba = 'Array(' + ', '.join(arg['vba'] or arg['name'] for arg in xlfunc['args']) + ')'

                if ftype == "Sub":
                    with vba.block('#If App = "Microsoft Excel" Then'):
                        vba.writeln('Py.CallUDF "{module_name}", "{fname}", {args_vba}, ThisWorkbook, Application.Caller',
                                    module_name=module_name,
                                    fname=fname,
                                    args_vba=args_vba,
                                    )
                    with vba.block("#Else"):
                        vba.writeln('Py.CallUDF "{module_name}", "{fname}", {args_vba}',
                                    module_name=module_name,
                                    fname=fname,
                                    args_vba=args_vba,
                                    )
                    vba.writeln("#End If")
                else:
                    with vba.block('#If App = "Microsoft Excel" Then'):
                        vba.writeln("If TypeOf Application.Caller Is Range Then On Error GoTo failed")
                        vba.writeln('{fname} = Py.CallUDF("{module_name}", "{fname}", {args_vba}, ThisWorkbook, Application.Caller)',
                                    module_name=module_name,
                                    fname=fname,
                                    args_vba=args_vba,
                                    )
                        vba.writeln("Exit " + ftype)
                    with vba.block("#Else"):
                        vba.writeln('{fname} = Py.CallUDF("{module_name}", "{fname}", {args_vba})',
                                module_name=module_name,
                                fname=fname,
                                args_vba=args_vba,
                                )
                        vba.writeln("Exit " + ftype)
                    vba.writeln("#End If")

                    vba.write_label("failed")
                    vba.writeln(fname + " = Err.Description")

            vba.writeln('End ' + ftype)
            vba.writeln('')


def import_udfs(module_names, xl_workbook):
    module_names = module_names.split(';')

    tf = tempfile.NamedTemporaryFile(mode='w', delete=False)

    vba = VBAWriter(tf.file)

    vba.writeln('Attribute VB_Name = "xlwings_udfs"')

    vba.writeln("'Autogenerated code by xlwings - changes will be lost with next import!")
    vba.writeln("""#Const App = "Microsoft Excel" 'Adjust when using outside of Excel""")

    for module_name in module_names:
        module = get_udf_module(module_name)
        generate_vba_wrapper(module_name, module, tf.file)

    tf.close()

    try:
        xl_workbook.VBProject.VBComponents.Remove(xl_workbook.VBProject.VBComponents("xlwings_udfs"))
    except:
        pass
    xl_workbook.VBProject.VBComponents.Import(tf.name)

    for module_name in module_names:
        module = get_udf_module(module_name)
        for mvar in map(lambda attr: getattr(module, attr), dir(module)):
            if hasattr(mvar, '__xlfunc__'):
                xlfunc = mvar.__xlfunc__
                xlret = xlfunc['ret']
                xlargs = xlfunc['args']
                fname = xlfunc['name']
                fdoc = xlret['doc'][:255]
                fcategory = xlfunc['category']

                excel_version = [int(x) for x in re.split("[,\\.]", xl_workbook.Application.Version)]
                if excel_version[0] >= 14:
                    argdocs = [arg['doc'][:255] for arg in xlargs if not arg['vba']]
                    xl_workbook.Application.MacroOptions("'" + xl_workbook.Name + "'!" + fname,
                                                         Description=fdoc,
                                                         HasMenu=False,
                                                         MenuText=None,
                                                         HasShortcutKey=False,
                                                         ShortcutKey=None,
                                                         Category=fcategory,
                                                         StatusBar=None,
                                                         HelpContextID=None,
                                                         HelpFile=None,
                                                         ArgumentDescriptions=argdocs if argdocs else None)
                else:
                    xl_workbook.Application.MacroOptions("'" + xl_workbook.Name + "'!" + fname, Description=fdoc)

    # try to delete the temp file - doesn't matter too much if it fails
    try:
        os.unlink(tf.name)
    except:
        pass
