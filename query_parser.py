# from __future__ import print_function
import re
import numpy as np

class Parser:

    # 0 ~ term
    # 1 ~ n_args
    # 2 ~ |
    # 3 ~ &
    # 4 ~ !

    def __init__(self, postlists_getter):
        """
        postlists_getter is func(list of terms) -> list of postlists on the same positions in list
        """
        self.rgx = re.compile('([&|!() ])')
        self.get_postlists = postlists_getter

    def parse(self, query):
        self.stack = []
        self.tokens = list(map(
            lambda s: s.decode('utf-8').lower().encode('utf-8'),
            re.sub(self.rgx, r' \1 ', query).split()
        ))
        self.n_tokens = len(self.tokens)
        self.start = 0
        self.terms = []
        self.postlists = []
        try:
            finite = self._rd_expr()
        except AssertionError:
            raise Exception('syntax error')
        if not finite:
            raise Exception('"infinite" query')
        if self._can_get_token():
            raise Exception('syntax error')

    def prepare_postlists(self):
        self.postlists = self.get_postlists(self.terms)

    def execute(self):
        # print("stack: ", self.stack)
        res, finite = self._rpn_op_exec(self.stack.pop())
        if self.stack:
            raise Exception('non empty stack')
        if not finite:
            raise Exception('bad query')
        return res

    def _get_token(self):
        token = self.tokens[self.start]
        self.start += 1
        return token

    def _can_get_token(self):
        return self.start < self.n_tokens

    def _view_token(self):
        return self.tokens[self.start]

    def _rd_opnd(self):
        assert(self._can_get_token())
        token = self._get_token()
        if token == '(':
            finite = self._rd_expr()
            assert(self._can_get_token() and self._get_token() == ')')
        else:
            assert(token not in ['|', '&', '!'])
            finite = True
            self.terms.append(token)
            self.stack.append((0, len(self.terms) - 1, finite))
        return finite

    def _rd_not(self):
        assert(self._can_get_token())
        token = self._view_token()
        if token == '!':
            self.start += 1
            finite = not self._rd_opnd()
            self.stack.append((4, None, finite))
        else:
            finite = self._rd_opnd()
        return finite

    def _rd_and(self):
        finite = self._rd_not()
        arg_n = 1
        while self._can_get_token():
            token = self._view_token()
            if token == '&':
                self.start += 1
            elif token in ['|', ')']:
                break
            if token == '(':
                finite |= self._opnd()
            else:
                finite |= self._rd_not()
            arg_n += 1
        if arg_n > 1:
            self.stack.extend([(1, arg_n, None), (3, None, finite)])
        return finite

    def _rd_expr(self):
        finite = self._rd_and()
        arg_n = 1
        while self._can_get_token() and self._view_token() == '|':
            self.start += 1
            finite &= self._rd_and()
            arg_n += 1
        if arg_n > 1:
            self.stack.extend([(1, arg_n, None), (2, None, finite)])
        return finite

    def pl_intersect(self, postlists_fin, postlists_inf):
        from functools import partial
        func = partial(np.intersect1d, assume_unique=True)
        inter_fin = reduce(func, postlists_fin) if postlists_fin else []
        inter_inf = reduce(func, postlists_inf) if postlists_inf else []
        return np.setdiff1d(inter_fin, inter_inf).astype('int64')

    def pl_union(self, postlists):
        return np.array(
            reduce(np.union1d, postlists)
        ).astype('int64') if postlists else np.array([])

    def _rpn_op_exec(self, op):
        type, arg, finite = op
        if type == 0:
            # print('executing type \'term\', arg =', arg)
            return self.postlists[arg], True
        elif type in {2, 3}:
            # print('executing type', '\'|\'' if type == 2 else '\'&\'')
            t, arg_n, _ = self.stack.pop()
            assert(t == 1)
            args_fin = []
            args_inf = []
            for k in xrange(arg_n):
                opnd = self.stack.pop()
                opnd_res, arg_finite = self._rpn_op_exec(opnd)
                if arg_finite:
                    args_fin.append(opnd_res)
                else:
                    args_inf.append(opnd_res)
            if type == 2:
                # |
                if finite:
                    assert(not args_inf)
                    return self.pl_union(args_fin), finite
                else:
                    assert(args_inf)
                    return self.pl_intersect(args_inf, args_fin), finite
            else:
                # &
                if finite:
                    assert(args_fin)
                    return self.pl_intersect(args_fin, args_inf), finite
                else:
                    assert(not args_fin)
                    return self.pl_union(args_inf), finite
        elif type == 4:
            # print('executing type \'!\'')
            opnd = self.stack.pop()
            res, finite = self._rpn_op_exec(opnd)
            return res, not finite
        else:
            raise Exception('execution error')
