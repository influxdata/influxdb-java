/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 azeti Networks AG (<info@azeti.net>)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package org.influxdb.impl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves generic type variables to actual types, based on context.
 *
 * @author Eran Leshem
 */
@FunctionalInterface
public interface TypeMapper {
  TypeMapper EMPTY = typeVariable -> null;

  static TypeMapper of(ParameterizedType type, Class<?> clazz) {
    TypeVariable<? extends Class<?>>[] typeVariables = clazz.getTypeParameters();
    Type[] types = type.getActualTypeArguments();
    if (types.length != typeVariables.length) {
      throw new IllegalStateException("Mismatched lengths for type variables and actual types");
    }
    Map<TypeVariable<?>, Type> typeMapping = new HashMap<>(typeVariables.length);
    for (int i = 0; i < typeVariables.length; i++) {
      typeMapping.put(typeVariables[i], types[i]);
    }

    return typeMapping::get;
  }

  static TypeMapper of(TypeMapper innerMapper, TypeMapper outerMapper) {
    return typeVariable -> map(typeVariable, innerMapper, outerMapper);
  }

  static TypeMapper empty() {
    return EMPTY;
  }

  default Type resolve(Type type) {
    if (type instanceof TypeVariable) {
      Type resolvedType = get((TypeVariable<?>) type);
      if (resolvedType == null) {
        throw new IllegalStateException("Could not resolve type " + type);
      }

      return resolvedType;
    }

    return type;
  }

  static Type map(TypeVariable<?> typeVariable, TypeMapper innerMapper, TypeMapper outerMapper) {
    Type type = innerMapper.get(typeVariable);
    if (type != null) {
      return type;
    }

    return outerMapper.get(typeVariable);
  }

  Type get(TypeVariable<?> typeVariable);
}
