import ctypes
import re
import sys
import os

class CHeaderParser:
    def __init__(self):
        # 基本 C 類型到 ctypes 的映射
        self.type_map = {
            'uint8_t': ctypes.c_uint8,
            'uint16_t': ctypes.c_uint16,
            'uint32_t': ctypes.c_uint32,
            'uint64_t': ctypes.c_uint64,
            'int8_t': ctypes.c_int8,
            'int16_t': ctypes.c_int16,
            'int32_t': ctypes.c_int32,
            'int64_t': ctypes.c_int64,
            'char': ctypes.c_char,
            'bool': ctypes.c_ubyte,
            'void': ctypes.c_uint64,
            'size_t': ctypes.c_size_t,
        }
        self.structs = {}  # 儲存已建立的 ctypes class
        self.pending_fields = []  # 暫存待處理的欄位資訊


    def _remove_comments(self, text):
        """移除 C 風格的註解 // 和 /* ... */"""
        def replacer(match):
            s = match.group(0)
            if s.startswith('/'):
                return " "
            else:
                return s
        pattern = re.compile(
            r'//.*?$|/\*.*?\*/|\'(?:\\.|[^\\\'])*\'|"(?:\\.|[^\\"])*"',
            re.DOTALL | re.MULTILINE
        )
        text = re.sub(pattern, replacer, text)
        # Remove macros that look like function calls but are types/fields
        text = re.sub(r'QTAILQ_ENTRY\s*\([^)]+\)', 'QTailQLink', text)
        text = re.sub(r'QTAILQ_HEAD\s*\([^)]+\)', 'QTailQLink', text)
        text = re.sub(r'QSIMPLEQ_ENTRY\s*\([^)]+\)', 'void *', text)
        text = re.sub(r'QSIMPLEQ_HEAD\s*\([^)]+\)', 'QTailQLink', text)
        return text

    def register_struct(self, name):
        """註冊一個新的 ctypes Structure，如果已存在則返回"""
        if name not in self.structs:
            # NVMe hardware structures and RPC/RPS mirrors are Little Endian
            if name.startswith("Nvme") or name.startswith("RPCNvme") or name.startswith("RPSNvme"):
                base = ctypes.LittleEndianStructure
            else:
                base = ctypes.BigEndianStructure
            class DynamicStruct(base):
                pass
            DynamicStruct.__name__ = name
            self.structs[name] = DynamicStruct
        return self.structs[name]

    def parse_file(self, file_path):
        """讀取並解析標頭檔"""
        print(f"Parsing {file_path}...")
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} not found.")
            return

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 前處理：移除註解並正規化空白
        content = self._remove_comments(content)
        content = ' '.join(content.split())

        # Use brace counting to correctly handle nested structs
        idx = 0
        n = len(content)
        
        # Regex to find the start of a struct definition
        start_pattern = re.compile(r'(typedef\s+)?struct\s*(?:(QEMU_PACKED)\s+)?(\w*)?\s*\{')

        while idx < n:
            match = start_pattern.search(content, idx)
            if not match:
                break
            
            start_idx = match.start()
            brace_open_idx = match.end() - 1
            
            # Find matching closing brace
            brace_count = 0
            brace_close_idx = -1
            for i in range(brace_open_idx, n):
                if content[i] == '{':
                    brace_count += 1
                elif content[i] == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        brace_close_idx = i
                        break
            
            if brace_close_idx == -1:
                break
            
            # Extract body
            body = content[brace_open_idx+1:brace_close_idx]
            
            # Extract suffix (up to semicolon)
            semi_idx = content.find(';', brace_close_idx)
            if semi_idx == -1:
                break
            
            suffix = content[brace_close_idx+1:semi_idx].strip()
            
            # Parse groups
            is_typedef = bool(match.group(1))
            is_packed_prefix = bool(match.group(2))
            struct_tag = match.group(3)
            
            if is_typedef:
                if suffix:
                    parts = suffix.split()
                    name = parts[-1]
                    cls = self.register_struct(name)
                    if struct_tag:
                        self.structs[struct_tag] = cls
                    self.pending_fields.append((cls, body, is_packed_prefix))
            else:
                if struct_tag:
                    is_packed = is_packed_prefix
                    if suffix and 'PACKED' in suffix:
                        is_packed = True
                    cls = self.register_struct(struct_tag)
                    self.pending_fields.append((cls, body, is_packed))
            
            idx = semi_idx + 1

    def _split_struct_body(self, body):
        """Splits a struct body into individual field definitions,
        correctly handling semicolons within nested struct/union definitions.
        """
        defs = []
        brace_level = 0
        current_def_start = 0
        for i, char in enumerate(body):
            if char == '{':
                brace_level += 1
            elif char == '}':
                brace_level -= 1
            elif char == ';' and brace_level == 0:
                defs.append(body[current_def_start:i].strip())
                current_def_start = i + 1
        last_def = body[current_def_start:].strip()
        if last_def:
            defs.append(last_def)
        return [d for d in defs if d]

    def finalize(self):
        """處理所有暫存的結構欄位，解決型別依賴"""
        pending = list(self.pending_fields)
        
        while pending:
            resolved_count = 0
            remaining = []

            for cls, body, is_packed in pending:
                can_resolve = True
                parsed_fields = []

                definitions = self._split_struct_body(body)
                for def_str in definitions:
                    # Handle anonymous inner structs like: `struct { ... } cfg`
                    # Also handle unions
                    if (def_str.startswith('struct') or def_str.startswith('union')) and '{' in def_str:
                        bstart = def_str.find('{')
                        depth = 0
                        bend = -1
                        for i, ch in enumerate(def_str[bstart:], start=bstart):
                            if ch == '{':
                                depth += 1
                            elif ch == '}':
                                depth -= 1
                                if depth == 0:
                                    bend = i
                                    break
                        
                        if bend != -1:
                            inner_body = def_str[bstart + 1:bend]
                            field_name_part = def_str[bend + 1:].strip()
                            # Clean up attributes
                            field_name_part = re.sub(r'QEMU_PACKED|PACKED|__attribute__\s*\(\(.*?\)\)', '', field_name_part).strip()
                            field_name_parts = field_name_part.split()
                            if field_name_parts:
                                var_name_full = field_name_parts[0]
                                gen_name = f"{cls.__name__}_{var_name_full}_anon"
                                if gen_name not in self.structs:
                                    AnonCls = self.register_struct(gen_name)
                                    self.pending_fields.append((AnonCls, inner_body, is_packed))
                                parts = ['struct', gen_name, var_name_full]
                            else:
                                parts = def_str.split()
                        else:
                            parts = def_str.split()
                    else:
                        parts = def_str.split()
                    if not parts: continue

                    is_struct_keyword = parts[0] == 'struct'
                    type_idx = 1 if is_struct_keyword else 0
                    name_idx = 2 if is_struct_keyword else 1

                    if len(parts) <= name_idx: continue
                    
                    type_name = parts[type_idx]
                    var_name_full = parts[name_idx]
                    
                    is_pointer = var_name_full.count('*')
                    var_name_full = var_name_full.lstrip('*')
                    array_len = 1
                    var_name = var_name_full
                    if '[' in var_name_full:
                        var_name, arr_part = var_name_full.split('[', 1)
                        arr_part = arr_part.replace(']', '')
                        try:
                            array_len = int(arr_part)
                        except ValueError:
                            array_len = 1
                    
                    field_info = {
                        'type_name': type_name,
                        'var_name': var_name,
                        'array_len': array_len,
                        'is_pointer': is_pointer > 0,
                    }
                    parsed_fields.append(field_info)

                    # 檢查依賴是否已解決
                    if not field_info['is_pointer'] and type_name not in self.type_map:
                        if type_name not in self.structs or not hasattr(self.structs[type_name], '_fields_'):
                            can_resolve = False
                            break

                if not can_resolve:
                    remaining.append((cls, body, is_packed))
                    continue

                # --- 可以解析，建立新的 Class ---
                
                final_fields = []
                for info in parsed_fields:
                    ctype = None
                    if info['is_pointer']:
                        # Treat all pointers as 64-bit integers for simplicity
                        ctype = ctypes.c_uint64
                    else:
                        if info['type_name'] in self.type_map:
                            ctype = self.type_map[info['type_name']]
                        elif info['type_name'] in self.structs:
                            ctype = self.structs[info['type_name']]
                        else:
                            ctype = ctypes.c_uint32 # Fallback
                        if info['array_len'] > 1:
                            ctype = ctype * info['array_len']
                    
                    final_fields.append((info['var_name'], ctype))

                # 建立新的 class definition
                class_dict = {'_fields_': final_fields}
                if is_packed:
                    class_dict['_pack_'] = 1
                
                NewCls = type(cls.__name__, cls.__bases__, class_dict)

                # 更新所有指向舊 placeholder 的引用
                aliases = [name for name, c in self.structs.items() if c == cls]
                for alias in aliases:
                    self.structs[alias] = NewCls

                resolved_count += 1
            
            if not resolved_count and remaining:
                print("Warning: Circular dependency or unresolved types for:", 
                      [c.__name__ for c, _, _ in remaining])
                break # 避免無限迴圈
            
            pending = remaining + self.pending_fields
            self.pending_fields.clear()

    def get_struct(self, name):
        return self.structs.get(name)

class remote_port_header:
    def __init__(self):
        self.parser = CHeaderParser()

        # Register stubs for QEMU internal types that are not parsed but used by value.
        # This allows structs embedding them (like RPCNvmeCtrl) to be resolved.
        for stub in ['PCIDevice', 'MemoryRegion', 'BlockConf']:
            self.parser.register_struct(stub)._fields_ = []

        base_dir = os.path.dirname(os.path.abspath(__file__))
        base_path_hw = os.path.join(base_dir, "header", "hw")
        base_path_block = os.path.join(base_dir, "header", "block")
        base_path_qemu = os.path.join(base_dir, "header", "qemu")

        # The order is important for dependency resolution. Parse definitions before they are used.
        self.parser.parse_file(os.path.join(base_path_qemu, "queue.h"))
        self.parser.parse_file(os.path.join(base_path_block, "nvme.h"))
        self.parser.parse_file(os.path.join(base_path_hw, "remote-port-proto.h"))
        self.parser.parse_file(os.path.join(base_path_hw, "remote-port-nvme-server.h"))
        self.parser.parse_file(os.path.join(base_path_hw, "remote-port-nvme-client.h"))
        self.parser.finalize()
        # Alias RPSNvmeCtrl to RPCNvmeCtrl if the latter is not defined.
        if 'RPSNvmeCtrl' in self.parser.structs and 'RPCNvmeCtrl' not in self.parser.structs:
            self.parser.structs['RPCNvmeCtrl'] = self.parser.structs['RPSNvmeCtrl']

    def __getattr__(self, name):
        return self.parser.get_struct(name)

    def make_struct_instance(self, name):
        """Create an instance of a parsed struct and auto-initialize nested
        fields (embedded structs, pointers and arrays) so client code can
        safely access nested members like `admin_cq.phase`.
        """
        cls = self.parser.get_struct(name)
        if cls is None:
            raise KeyError(f"struct {name!r} not found in parsed headers")

        inst = cls()
        # Auto-initialize embedded struct fields (value-types), pointer
        # fields and arrays.
        fields = getattr(cls, '_fields_', [])
        for fname, ftype in fields:
            try:
                # Embedded struct (direct value)
                if isinstance(ftype, type) and issubclass(ftype, ctypes.Structure):
                    try:
                        # assign a fresh instance to ensure attribute exists
                        setattr(inst, fname, ftype())
                    except Exception:
                        pass

                # Pointer to struct: POINTER(SomeStruct)
                elif hasattr(ftype, '_type_') and isinstance(ftype._type_, type) and issubclass(ftype._type_, ctypes.Structure):
                    nested_cls = ftype._type_
                    try:
                        setattr(inst, fname, ctypes.pointer(nested_cls()))
                    except Exception:
                        pass

                # Array of structs: SomeStruct * N
                elif hasattr(ftype, '_length_') and hasattr(ftype, '_type_') and isinstance(ftype._type_, type) and issubclass(ftype._type_, ctypes.Structure):
                    arr_cls = ftype._type_
                    length = getattr(ftype, '_length_', 0)
                    try:
                        setattr(inst, fname, (arr_cls * length)())
                    except Exception:
                        pass
            except Exception:
                continue

        return inst

class remote_port_proto:
    def __init__(self):
        self.parser = CHeaderParser()
        base_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = os.path.join(base_dir, "header", "hw")
        proto_h = os.path.join(base_path, "remote-port-proto.h")
        self.parser.parse_file(proto_h)
        self.parser.finalize()

    def __getattr__(self, name):
        return self.parser.get_struct(name)
  
class remote_port_nvme_server:
    def __init__(self):
        self.parser = CHeaderParser()
        base_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = os.path.join(base_dir, "header", "hw")
        nvme_h = os.path.join(base_path, "remote-port-nvme-server.h")
        self.parser.parse_file(nvme_h)
        self.parser.finalize()

    def __getattr__(self, name):
        return self.parser.get_struct(name)

class remote_port_nvme_client:
    def __init__(self):
        self.parser = CHeaderParser()
        base_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = os.path.join(base_dir, "header", "hw")
        nvme_h = os.path.join(base_path, "remote-port-nvme-client.h")
        self.parser.parse_file(nvme_h)
        self.parser.finalize()

    def __getattr__(self, name):
        return self.parser.get_struct(name)
    
    def make_struct_instance(self, name):
        """Create and return an instance of the parsed struct named `name`.
        This helper also attempts to auto-initialize nested fields such as
        queue structs (pointer and array-aware). Use this from client code
        when nested sub-structs are not automatically allocated.
        """
        cls = self.parser.get_struct(name)
        if cls is None:
            raise KeyError(f"struct {name!r} not found in parsed headers")

        inst = cls()
        self._auto_init_nested(inst)
        return inst

    def _auto_init_nested(self, inst):
        """Auto-allocate nested struct pointers and arrays on `inst`.

        Behavior:
        - If a field is a POINTER(<Struct>), allocate an instance of <Struct>
          and assign `ctypes.pointer(instance)` to the field.
        - If a field is an array of a Struct type (e.g. <Struct> * N), allocate
          the array and assign it.

        This is conservative and ignores fields that fail to allocate.
        """
        # Be defensive if the class has no _fields_
        fields = getattr(inst.__class__, '_fields_', [])
        for fname, ftype in fields:
            try:
                # Case: POINTER(SomeStruct)
                if hasattr(ftype, '_type_') and isinstance(ftype._type_, type) and issubclass(ftype._type_, ctypes.Structure):
                    nested_cls = ftype._type_
                    nested = nested_cls()
                    # assign a pointer to the nested instance
                    try:
                        setattr(inst, fname, ctypes.pointer(nested))
                    except (TypeError, AttributeError):
                        # fallback: try assigning the struct directly (some
                        # layouts embed structs directly)
                        try:
                            setattr(inst, fname, nested)
                        except Exception:
                            pass

                # Case: array of Structs (e.g. SomeStruct * N)
                elif hasattr(ftype, '_length_') and hasattr(ftype, '_type_') and isinstance(ftype._type_, type) and issubclass(ftype._type_, ctypes.Structure):
                    arr_cls = ftype._type_
                    length = getattr(ftype, '_length_', 0)
                    arr = (arr_cls * length)()
                    try:
                        setattr(inst, fname, arr)
                    except Exception:
                        pass
            except Exception:
                # Never fail hard from this helper; it's best-effort
                continue

    def make_controller(self):
        """Convenience: create a controller instance and try to allocate
        commonly used nested queues (`admin_cq`, `admin_sq`) when present.
        """
        # Try a few likely controller struct names; return first that exists
        for cand in ('RPCNvmeController', 'RPCNvmeCtrl', 'RPCNvmeDevice'):
            if self.parser.get_struct(cand):
                ctrl = self.make_struct_instance(cand)
                # Some client code expects direct struct attributes rather
                # than POINTER(...). Try to ensure `admin_cq`/`admin_sq` are
                # usable in both styles.
                for qname, qtype in (('admin_cq', 'RPCNvmeCQueue'), ('admin_sq', 'RPCNvmeSQueue')):
                    if hasattr(ctrl, qname):
                        # if it's a pointer, leave as-is (user can use .contents)
                        continue
                    # attempt to set a plain instance attribute for convenience
                    qcls = self.parser.get_struct(qtype)
                    if qcls:
                        try:
                            setattr(ctrl, qname, qcls())
                        except Exception:
                            pass
                return ctrl
        raise KeyError('no known RPCNvmeController-like struct found')
        
class nvme:
    def __init__(self):
        self.parser = CHeaderParser()
        base_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = os.path.join(base_dir, "header", "block")
        nvme_h = os.path.join(base_path, "nvme.h")
        self.parser.parse_file(nvme_h)
        self.parser.finalize()

    def __getattr__(self, name):
        return self.parser.get_struct(name)
