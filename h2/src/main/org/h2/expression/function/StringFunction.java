/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.Mode.ModeEnum;

import java.util.HashSet;
import java.util.Set;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * An string function with multiple arguments.
 */
public final class StringFunction extends FunctionN {

    /**
     * LOCATE() (non-standard).
     */
    public static final int LOCATE = 0;

    /**
     * INSERT() (non-standard).
     */
    public static final int INSERT = LOCATE + 1;

    /**
     * REPLACE() (non-standard).
     */
    public static final int REPLACE = INSERT + 1;

    /**
     * LPAD() (non-standard).
     */
    public static final int LPAD = REPLACE + 1;

    /**
     * RPAD() (non-standard).
     */
    public static final int RPAD = LPAD + 1;

    /**
     * TRANSLATE() (non-standard).
     */
    public static final int TRANSLATE = RPAD + 1;

    /**
     * LEVENSHTEIN() (non-standard).
     */
    public static final int LEVENSHTEIN = TRANSLATE + 1;


    private static final String[] NAMES = { //
            "LOCATE", "INSERT", "REPLACE", "LPAD", "RPAD", "TRANSLATE", "LEVENSHTEIN" //
    };

    private final int function;

    private Set<String> levenshteinAlgorithms; 

    public StringFunction(Expression arg1, Expression arg2, Expression arg3, int function) {
        super(arg3 == null ? new Expression[] { arg1, arg2 } : new Expression[] { arg1, arg2, arg3 });
        this.function = function;
        levenshteinAlgorithms = new HashSet<String> ();
        levenshteinAlgorithms.add("wagner");
        levenshteinAlgorithms.add("prefsuf");
        levenshteinAlgorithms.add("recursive");
        levenshteinAlgorithms.add("optimized_wagner");
    }

    public StringFunction(Expression arg1, Expression arg2, Expression arg3, Expression arg4, int function) {
        super(new Expression[] { arg1, arg2, arg3, arg4 });
        this.function = function;
        levenshteinAlgorithms = new HashSet<String> ();
        levenshteinAlgorithms.add("wagner");
        levenshteinAlgorithms.add("prefsuf");
        levenshteinAlgorithms.add("recursive");
        levenshteinAlgorithms.add("optimized_wagner");
    }

    public StringFunction(Expression[] args, int function) {
        super(args);
        this.function = function;
        levenshteinAlgorithms = new HashSet<String> ();
        levenshteinAlgorithms.add("wagner");
        levenshteinAlgorithms.add("prefsuf");
        levenshteinAlgorithms.add("recursive");
        levenshteinAlgorithms.add("optimized_wagner");
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v1 = args[0].getValue(session), v2 = args[1].getValue(session);
        switch (function) {
        case LOCATE: {
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            Value v3 = args.length >= 3 ? args[2].getValue(session) : null;
            if (v3 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            v1 = ValueInteger.get(locate(v1.getString(), v2.getString(), v3 == null ? 1 : v3.getInt()));
            break;
        }
        case INSERT: {
            Value v3 = args[2].getValue(session), v4 = args[3].getValue(session);
            if (v2 != ValueNull.INSTANCE && v3 != ValueNull.INSTANCE) {
                String s = insert(v1.getString(), v2.getInt(), v3.getInt(), v4.getString());
                v1 = s != null ? ValueVarchar.get(s, session) : ValueNull.INSTANCE;
            }
            break;
        }
        case REPLACE: {
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            String after;
            if (args.length >= 3) {
                Value v3 = args[2].getValue(session);
                if (v3 == ValueNull.INSTANCE && session.getMode().getEnum() != ModeEnum.Oracle) {
                    return ValueNull.INSTANCE;
                }
                after = v3.getString();
                if (after == null) {
                    after = "";
                }
            } else {
                after = "";
            }
            v1 = ValueVarchar.get(StringUtils.replaceAll(v1.getString(), v2.getString(), after), session);
            break;
        }
        case LPAD:
        case RPAD:
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            String padding;
            if (args.length >= 3) {
                Value v3 = args[2].getValue(session);
                if (v3 == ValueNull.INSTANCE) {
                    return ValueNull.INSTANCE;
                }
                padding = v3.getString();
            } else {
                padding = null;
            }
            v1 = ValueVarchar.get(StringUtils.pad(v1.getString(), v2.getInt(), padding, function == RPAD), session);
            break;
        case TRANSLATE: {
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            Value v3 = args[2].getValue(session);
            if (v3 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            String matching = v2.getString();
            String replacement = v3.getString();
            if (session.getMode().getEnum() == ModeEnum.DB2) {
                String t = matching;
                matching = replacement;
                replacement = t;
            }
            v1 = ValueVarchar.get(translate(v1.getString(), matching, replacement), session);
            break;
        }
        case LEVENSHTEIN:
            Value v3 = args[2].getValue(session);
            String algorithm;
            if (v3 == ValueNull.INSTANCE) {
                algorithm = "wagner";
            } else {
                algorithm = v3.getString().toLowerCase();
            }
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE || (!levenshteinAlgorithms.contains(algorithm))) {
                return ValueNull.INSTANCE;
            }
            String string1 = v1.getString();
            String string2 = v2.getString();
            if (Math.min(string1.length(), string2.length())==0) {
                return ValueInteger.get(Math.max(string1.length(), string2.length()));
            }
            switch (algorithm) {

                case "wagner": {
                    v1 = ValueInteger.get(wagner(string1, string2));
                    break;
                }

                case "prefsuf": {
                    v1 = ValueInteger.get(prefSuf(string1, string2));
                    break;
                }

                case "optimized_wagner": {
                    v1 = ValueInteger.get(optimizedWagner(string1, string2));
                    break;
                }

                case "recursive": {
                    v1 = ValueInteger.get(recursive(string1, string2));
                    break;
                }
            }
            break;            

        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v1;
    }

    private static int wagner(String s1, String s2) {
		int d[][] = new int[s1.length() + 1][s2.length() + 1];
		
		// Initialising first column:
		for(int i = 0; i <= s1.length(); i++)
			d[i][0] = i;
		
		// Initialising first row:
		for(int j = 0; j <= s2.length(); j++)
			d[0][j] = j;
		
		// Applying the algorithm:
		int insertion, deletion, replacement;
		for(int i = 1; i <= s1.length(); i++) {
			for(int j = 1; j <= s2.length(); j++) {
				if(s1.charAt(i - 1) == (s2.charAt(j - 1)))
					d[i][j] = d[i - 1][j - 1];
				else {
					insertion = d[i][j - 1];
					deletion = d[i - 1][j];
					replacement = d[i - 1][j - 1];
					
					// Using the sub-problems
					d[i][j] = 1 + findMin(insertion, deletion, replacement);
				}
			}
		}
		
		return d[s1.length()][s2.length()];
	}

    public static int optimizedWagner(String word1, String word2){
        int len1,len2,indi,prev_dia,prev_abov,i,j;
        int [] buffer;
        prev_abov=0;
        len1 = word1.length();
        len2 = word2.length();
        buffer = new int [len2+1];
        for (j = 0; j < len2+1; j++){
            buffer[j] = j;
        }
        for (i=0; i<=len1-1;i++){
            buffer[0]=i+1;
            for (j = 0; j <= len2-1; j++){
                indi = ((word1.charAt(i) != word2.charAt(j)) ? 1 : 0);
                prev_dia = prev_abov;
                prev_abov = buffer [j+1];
                buffer[j+1] = Math.min(Math.min(prev_abov+1,buffer[j]+1),prev_dia+indi);
            }
        }
        return buffer [len2];
    }

    public static int mismatch (String a, String b){
        for(int i=0; i<b.length(); i++){
            if(a.charAt(i) != b.charAt(i))
            {
                return i;
            }
        }
        return b.length();
    }

    public static String reverseString(String str){  
        StringBuilder sb=new StringBuilder(str);  
        sb.reverse();  
        return sb.toString();  
    }  

    public static int prefSuf(String word1, String word2){
        int prefix,suffix;

        if (word1.length() < word2.length()){
            return prefSuf (word2, word1);
        }

        prefix = mismatch (word1, word2);

        word1 = word1.substring(prefix);
        word2 = word2.substring(prefix);

        suffix = mismatch(reverseString(word1), reverseString(word2));

        word1 = word1.substring(0,word1.length()- suffix);
        word2 = word2.substring(0,word2.length()- suffix);

        if (word1.length()==0){
            return word2.length();
        }
        else if (word2.length()==0){
            return word1.length();
        }
        return optimizedWagner(word1,word2);
    }

	
  // Helper funciton used by findDistance()
	private static int findMin(int x, int y, int z) {
		if(x <= y && x <= z)
			return x;
		if(y <= x && y <= z)
			return y;
		else
			return z;
	}

    private static int recursive(String string1,String string2) {

        if (Math.min(string1.length(), string2.length())==0) {
            return Math.max(string1.length(), string2.length());
        }

        int replace = recursive(string1.substring(1), string2.substring(1)) + isCharacterDif(string1.charAt(0), string2.charAt(0));

        int insert = recursive(string1, string2.substring(1))+ 1;

        int delete = recursive(string1.substring(1), string2)+ 1;

        return findMin(replace, insert, delete);
    }

    static int isCharacterDif(char c1, char c2) {
        return c1 == c2 ? 0 : 1;
    }


    private static int locate(String search, String s, int start) {
        if (start < 0) {
            return s.lastIndexOf(search, s.length() + start) + 1;
        }
        return s.indexOf(search, start == 0 ? 0 : start - 1) + 1;
    }

    private static String insert(String s1, int start, int length, String s2) {
        if (s1 == null) {
            return s2;
        }
        if (s2 == null) {
            return s1;
        }
        int len1 = s1.length();
        int len2 = s2.length();
        start--;
        if (start < 0 || length <= 0 || len2 == 0 || start > len1) {
            return s1;
        }
        if (start + length > len1) {
            length = len1 - start;
        }
        return s1.substring(0, start) + s2 + s1.substring(start + length);
    }

    private static String translate(String original, String findChars, String replaceChars) {
        if (StringUtils.isNullOrEmpty(original) || StringUtils.isNullOrEmpty(findChars)) {
            return original;
        }
        // if it stays null, then no replacements have been made
        StringBuilder builder = null;
        // if shorter than findChars, then characters are removed
        // (if null, we don't access replaceChars at all)
        int replaceSize = replaceChars == null ? 0 : replaceChars.length();
        for (int i = 0, size = original.length(); i < size; i++) {
            char ch = original.charAt(i);
            int index = findChars.indexOf(ch);
            if (index >= 0) {
                if (builder == null) {
                    builder = new StringBuilder(size);
                    if (i > 0) {
                        builder.append(original, 0, i);
                    }
                }
                if (index < replaceSize) {
                    ch = replaceChars.charAt(index);
                }
            }
            if (builder != null) {
                builder.append(ch);
            }
        }
        return builder == null ? original : builder.toString();
    }

    @Override
    public Expression optimize(SessionLocal session) {
        boolean allConst = optimizeArguments(session, true);
        switch (function) {
        case LOCATE:
        case LEVENSHTEIN:
            type = TypeInfo.TYPE_INTEGER;
            break;
        case INSERT:
        case REPLACE:
        case LPAD:
        case RPAD:
        case TRANSLATE:
            type = TypeInfo.TYPE_VARCHAR;
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        if (allConst) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
