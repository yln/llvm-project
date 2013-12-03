//===--- SemaDeclAttr.cpp - Declaration Attribute Handling ----------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
//  This file implements decl-related attribute processing.
//
//===----------------------------------------------------------------------===//

#include "clang/Sema/SemaInternal.h"
#include "TargetAttributesSema.h"
#include "clang/AST/ASTContext.h"
#include "clang/AST/CXXInheritance.h"
#include "clang/AST/DeclCXX.h"
#include "clang/AST/DeclObjC.h"
#include "clang/AST/DeclTemplate.h"
#include "clang/AST/Expr.h"
#include "clang/AST/Mangle.h"
#include "clang/Basic/CharInfo.h"
#include "clang/Basic/SourceManager.h"
#include "clang/Basic/TargetInfo.h"
#include "clang/Lex/Preprocessor.h"
#include "clang/Sema/DeclSpec.h"
#include "clang/Sema/DelayedDiagnostic.h"
#include "clang/Sema/Lookup.h"
#include "clang/Sema/Scope.h"
#include "llvm/ADT/StringExtras.h"
using namespace clang;
using namespace sema;

namespace AttributeLangSupport {
  enum LANG {
    C,
    Cpp,
    ObjC
  };
}

//===----------------------------------------------------------------------===//
//  Helper functions
//===----------------------------------------------------------------------===//

static const FunctionType *getFunctionType(const Decl *D,
                                           bool blocksToo = true) {
  QualType Ty;
  if (const ValueDecl *decl = dyn_cast<ValueDecl>(D))
    Ty = decl->getType();
  else if (const TypedefNameDecl* decl = dyn_cast<TypedefNameDecl>(D))
    Ty = decl->getUnderlyingType();
  else
    return 0;

  if (Ty->isFunctionPointerType())
    Ty = Ty->getAs<PointerType>()->getPointeeType();
  else if (blocksToo && Ty->isBlockPointerType())
    Ty = Ty->getAs<BlockPointerType>()->getPointeeType();

  return Ty->getAs<FunctionType>();
}

// FIXME: We should provide an abstraction around a method or function
// to provide the following bits of information.

/// isFunction - Return true if the given decl has function
/// type (function or function-typed variable).
static bool isFunction(const Decl *D) {
  return getFunctionType(D, false) != NULL;
}

/// isFunctionOrMethod - Return true if the given decl has function
/// type (function or function-typed variable) or an Objective-C
/// method.
static bool isFunctionOrMethod(const Decl *D) {
  return isFunction(D) || isa<ObjCMethodDecl>(D);
}

/// isFunctionOrMethodOrBlock - Return true if the given decl has function
/// type (function or function-typed variable) or an Objective-C
/// method or a block.
static bool isFunctionOrMethodOrBlock(const Decl *D) {
  if (isFunctionOrMethod(D))
    return true;
  // check for block is more involved.
  if (const VarDecl *V = dyn_cast<VarDecl>(D)) {
    QualType Ty = V->getType();
    return Ty->isBlockPointerType();
  }
  return isa<BlockDecl>(D);
}

/// Return true if the given decl has a declarator that should have
/// been processed by Sema::GetTypeForDeclarator.
static bool hasDeclarator(const Decl *D) {
  // In some sense, TypedefDecl really *ought* to be a DeclaratorDecl.
  return isa<DeclaratorDecl>(D) || isa<BlockDecl>(D) || isa<TypedefNameDecl>(D) ||
         isa<ObjCPropertyDecl>(D);
}

/// hasFunctionProto - Return true if the given decl has a argument
/// information. This decl should have already passed
/// isFunctionOrMethod or isFunctionOrMethodOrBlock.
static bool hasFunctionProto(const Decl *D) {
  if (const FunctionType *FnTy = getFunctionType(D))
    return isa<FunctionProtoType>(FnTy);
  else {
    assert(isa<ObjCMethodDecl>(D) || isa<BlockDecl>(D));
    return true;
  }
}

/// getFunctionOrMethodNumArgs - Return number of function or method
/// arguments. It is an error to call this on a K&R function (use
/// hasFunctionProto first).
static unsigned getFunctionOrMethodNumArgs(const Decl *D) {
  if (const FunctionType *FnTy = getFunctionType(D))
    return cast<FunctionProtoType>(FnTy)->getNumArgs();
  if (const BlockDecl *BD = dyn_cast<BlockDecl>(D))
    return BD->getNumParams();
  return cast<ObjCMethodDecl>(D)->param_size();
}

static QualType getFunctionOrMethodArgType(const Decl *D, unsigned Idx) {
  if (const FunctionType *FnTy = getFunctionType(D))
    return cast<FunctionProtoType>(FnTy)->getArgType(Idx);
  if (const BlockDecl *BD = dyn_cast<BlockDecl>(D))
    return BD->getParamDecl(Idx)->getType();

  return cast<ObjCMethodDecl>(D)->param_begin()[Idx]->getType();
}

static QualType getFunctionOrMethodResultType(const Decl *D) {
  if (const FunctionType *FnTy = getFunctionType(D))
    return cast<FunctionProtoType>(FnTy)->getResultType();
  return cast<ObjCMethodDecl>(D)->getResultType();
}

static bool isFunctionOrMethodVariadic(const Decl *D) {
  if (const FunctionType *FnTy = getFunctionType(D)) {
    const FunctionProtoType *proto = cast<FunctionProtoType>(FnTy);
    return proto->isVariadic();
  } else if (const BlockDecl *BD = dyn_cast<BlockDecl>(D))
    return BD->isVariadic();
  else {
    return cast<ObjCMethodDecl>(D)->isVariadic();
  }
}

static bool isInstanceMethod(const Decl *D) {
  if (const CXXMethodDecl *MethodDecl = dyn_cast<CXXMethodDecl>(D))
    return MethodDecl->isInstance();
  return false;
}

static inline bool isNSStringType(QualType T, ASTContext &Ctx) {
  const ObjCObjectPointerType *PT = T->getAs<ObjCObjectPointerType>();
  if (!PT)
    return false;

  ObjCInterfaceDecl *Cls = PT->getObjectType()->getInterface();
  if (!Cls)
    return false;

  IdentifierInfo* ClsName = Cls->getIdentifier();

  // FIXME: Should we walk the chain of classes?
  return ClsName == &Ctx.Idents.get("NSString") ||
         ClsName == &Ctx.Idents.get("NSMutableString");
}

static inline bool isCFStringType(QualType T, ASTContext &Ctx) {
  const PointerType *PT = T->getAs<PointerType>();
  if (!PT)
    return false;

  const RecordType *RT = PT->getPointeeType()->getAs<RecordType>();
  if (!RT)
    return false;

  const RecordDecl *RD = RT->getDecl();
  if (RD->getTagKind() != TTK_Struct)
    return false;

  return RD->getIdentifier() == &Ctx.Idents.get("__CFString");
}

static unsigned getNumAttributeArgs(const AttributeList &Attr) {
  // FIXME: Include the type in the argument list.
  return Attr.getNumArgs() + Attr.hasParsedType();
}

/// \brief Check if the attribute has exactly as many args as Num. May
/// output an error.
static bool checkAttributeNumArgs(Sema &S, const AttributeList &Attr,
                                  unsigned Num) {
  if (getNumAttributeArgs(Attr) != Num) {
    S.Diag(Attr.getLoc(), diag::err_attribute_wrong_number_arguments)
      << Attr.getName() << Num;
    return false;
  }

  return true;
}


/// \brief Check if the attribute has at least as many args as Num. May
/// output an error.
static bool checkAttributeAtLeastNumArgs(Sema &S, const AttributeList &Attr,
                                         unsigned Num) {
  if (getNumAttributeArgs(Attr) < Num) {
    S.Diag(Attr.getLoc(), diag::err_attribute_too_few_arguments) << Num;
    return false;
  }

  return true;
}

/// \brief If Expr is a valid integer constant, get the value of the integer
/// expression and return success or failure. May output an error.
static bool checkUInt32Argument(Sema &S, const AttributeList &Attr,
                                const Expr *Expr, uint32_t &Val,
                                unsigned Idx = UINT_MAX) {
  llvm::APSInt I(32);
  if (Expr->isTypeDependent() || Expr->isValueDependent() ||
      !Expr->isIntegerConstantExpr(I, S.Context)) {
    if (Idx != UINT_MAX)
      S.Diag(Attr.getLoc(), diag::err_attribute_argument_n_type)
        << Attr.getName() << Idx << AANT_ArgumentIntegerConstant
        << Expr->getSourceRange();
    else
      S.Diag(Attr.getLoc(), diag::err_attribute_argument_type)
        << Attr.getName() << AANT_ArgumentIntegerConstant
        << Expr->getSourceRange();
    return false;
  }
  Val = (uint32_t)I.getZExtValue();
  return true;
}

/// \brief Diagnose mutually exclusive attributes when present on a given
/// declaration. Returns true if diagnosed.
template <typename AttrTy>
static bool checkAttrMutualExclusion(Sema &S, Decl *D,
                                     const AttributeList &Attr,
                                     const char *OtherName) {
  // FIXME: it would be nice if OtherName did not have to be passed in, but was
  // instead determined based on the AttrTy template parameter.
  if (D->hasAttr<AttrTy>()) {
    S.Diag(Attr.getLoc(), diag::err_attributes_are_not_compatible)
      << Attr.getName() << OtherName;
    return true;
  }
  return false;
}

/// \brief Check if IdxExpr is a valid argument index for a function or
/// instance method D.  May output an error.
///
/// \returns true if IdxExpr is a valid index.
static bool checkFunctionOrMethodArgumentIndex(Sema &S, const Decl *D,
                                               StringRef AttrName,
                                               SourceLocation AttrLoc,
                                               unsigned AttrArgNum,
                                               const Expr *IdxExpr,
                                               uint64_t &Idx)
{
  assert(isFunctionOrMethod(D));

  // In C++ the implicit 'this' function parameter also counts.
  // Parameters are counted from one.
  bool HP = hasFunctionProto(D);
  bool HasImplicitThisParam = isInstanceMethod(D);
  bool IV = HP && isFunctionOrMethodVariadic(D);
  unsigned NumArgs = (HP ? getFunctionOrMethodNumArgs(D) : 0) +
                     HasImplicitThisParam;

  llvm::APSInt IdxInt;
  if (IdxExpr->isTypeDependent() || IdxExpr->isValueDependent() ||
      !IdxExpr->isIntegerConstantExpr(IdxInt, S.Context)) {
    std::string Name = std::string("'") + AttrName.str() + std::string("'");
    S.Diag(AttrLoc, diag::err_attribute_argument_n_type) << Name.c_str()
      << AttrArgNum << AANT_ArgumentIntegerConstant << IdxExpr->getSourceRange();
    return false;
  }

  Idx = IdxInt.getLimitedValue();
  if (Idx < 1 || (!IV && Idx > NumArgs)) {
    S.Diag(AttrLoc, diag::err_attribute_argument_out_of_bounds)
      << AttrName << AttrArgNum << IdxExpr->getSourceRange();
    return false;
  }
  Idx--; // Convert to zero-based.
  if (HasImplicitThisParam) {
    if (Idx == 0) {
      S.Diag(AttrLoc,
             diag::err_attribute_invalid_implicit_this_argument)
        << AttrName << IdxExpr->getSourceRange();
      return false;
    }
    --Idx;
  }

  return true;
}

/// \brief Check if the argument \p ArgNum of \p Attr is a ASCII string literal.
/// If not emit an error and return false. If the argument is an identifier it
/// will emit an error with a fixit hint and treat it as if it was a string
/// literal.
bool Sema::checkStringLiteralArgumentAttr(const AttributeList &Attr,
                                          unsigned ArgNum, StringRef &Str,
                                          SourceLocation *ArgLocation) {
  // Look for identifiers. If we have one emit a hint to fix it to a literal.
  if (Attr.isArgIdent(ArgNum)) {
    IdentifierLoc *Loc = Attr.getArgAsIdent(ArgNum);
    Diag(Loc->Loc, diag::err_attribute_argument_type)
        << Attr.getName() << AANT_ArgumentString
        << FixItHint::CreateInsertion(Loc->Loc, "\"")
        << FixItHint::CreateInsertion(PP.getLocForEndOfToken(Loc->Loc), "\"");
    Str = Loc->Ident->getName();
    if (ArgLocation)
      *ArgLocation = Loc->Loc;
    return true;
  }

  // Now check for an actual string literal.
  Expr *ArgExpr = Attr.getArgAsExpr(ArgNum);
  StringLiteral *Literal = dyn_cast<StringLiteral>(ArgExpr->IgnoreParenCasts());
  if (ArgLocation)
    *ArgLocation = ArgExpr->getLocStart();

  if (!Literal || !Literal->isAscii()) {
    Diag(ArgExpr->getLocStart(), diag::err_attribute_argument_type)
        << Attr.getName() << AANT_ArgumentString;
    return false;
  }

  Str = Literal->getString();
  return true;
}

/// \brief Applies the given attribute to the Decl without performing any
/// additional semantic checking.
template <typename AttrType>
static void handleSimpleAttribute(Sema &S, Decl *D,
                                  const AttributeList &Attr) {
  D->addAttr(::new (S.Context) AttrType(Attr.getRange(), S.Context,
                                        Attr.getAttributeSpellingListIndex()));
}

/// \brief Check if the passed-in expression is of type int or bool.
static bool isIntOrBool(Expr *Exp) {
  QualType QT = Exp->getType();
  return QT->isBooleanType() || QT->isIntegerType();
}


// Check to see if the type is a smart pointer of some kind.  We assume
// it's a smart pointer if it defines both operator-> and operator*.
static bool threadSafetyCheckIsSmartPointer(Sema &S, const RecordType* RT) {
  DeclContextLookupConstResult Res1 = RT->getDecl()->lookup(
    S.Context.DeclarationNames.getCXXOperatorName(OO_Star));
  if (Res1.empty())
    return false;

  DeclContextLookupConstResult Res2 = RT->getDecl()->lookup(
    S.Context.DeclarationNames.getCXXOperatorName(OO_Arrow));
  if (Res2.empty())
    return false;

  return true;
}

/// \brief Check if passed in Decl is a pointer type.
/// Note that this function may produce an error message.
/// \return true if the Decl is a pointer type; false otherwise
static bool threadSafetyCheckIsPointer(Sema &S, const Decl *D,
                                       const AttributeList &Attr) {
  if (const ValueDecl *vd = dyn_cast<ValueDecl>(D)) {
    QualType QT = vd->getType();
    if (QT->isAnyPointerType())
      return true;

    if (const RecordType *RT = QT->getAs<RecordType>()) {
      // If it's an incomplete type, it could be a smart pointer; skip it.
      // (We don't want to force template instantiation if we can avoid it,
      // since that would alter the order in which templates are instantiated.)
      if (RT->isIncompleteType())
        return true;

      if (threadSafetyCheckIsSmartPointer(S, RT))
        return true;
    }

    S.Diag(Attr.getLoc(), diag::warn_thread_attribute_decl_not_pointer)
      << Attr.getName()->getName() << QT;
  } else {
    S.Diag(Attr.getLoc(), diag::err_attribute_can_be_applied_only_to_value_decl)
      << Attr.getName();
  }
  return false;
}

/// \brief Checks that the passed in QualType either is of RecordType or points
/// to RecordType. Returns the relevant RecordType, null if it does not exit.
static const RecordType *getRecordType(QualType QT) {
  if (const RecordType *RT = QT->getAs<RecordType>())
    return RT;

  // Now check if we point to record type.
  if (const PointerType *PT = QT->getAs<PointerType>())
    return PT->getPointeeType()->getAs<RecordType>();

  return 0;
}


static bool checkBaseClassIsLockableCallback(const CXXBaseSpecifier *Specifier,
                                             CXXBasePath &Path, void *Unused) {
  const RecordType *RT = Specifier->getType()->getAs<RecordType>();
  if (RT->getDecl()->getAttr<LockableAttr>())
    return true;
  return false;
}


/// \brief Thread Safety Analysis: Checks that the passed in RecordType
/// resolves to a lockable object.
static void checkForLockableRecord(Sema &S, Decl *D, const AttributeList &Attr,
                                   QualType Ty) {
  const RecordType *RT = getRecordType(Ty);

  // Warn if could not get record type for this argument.
  if (!RT) {
    S.Diag(Attr.getLoc(), diag::warn_thread_attribute_argument_not_class)
      << Attr.getName() << Ty.getAsString();
    return;
  }

  // Don't check for lockable if the class hasn't been defined yet.
  if (RT->isIncompleteType())
    return;

  // Allow smart pointers to be used as lockable objects.
  // FIXME -- Check the type that the smart pointer points to.
  if (threadSafetyCheckIsSmartPointer(S, RT))
    return;

  // Check if the type is lockable.
  RecordDecl *RD = RT->getDecl();
  if (RD->getAttr<LockableAttr>())
    return;

  // Else check if any base classes are lockable.
  if (CXXRecordDecl *CRD = dyn_cast<CXXRecordDecl>(RD)) {
    CXXBasePaths BPaths(false, false);
    if (CRD->lookupInBases(checkBaseClassIsLockableCallback, 0, BPaths))
      return;
  }

  S.Diag(Attr.getLoc(), diag::warn_thread_attribute_argument_not_lockable)
    << Attr.getName() << Ty.getAsString();
}

/// \brief Thread Safety Analysis: Checks that all attribute arguments, starting
/// from Sidx, resolve to a lockable object.
/// \param Sidx The attribute argument index to start checking with.
/// \param ParamIdxOk Whether an argument can be indexing into a function
/// parameter list.
static void checkAttrArgsAreLockableObjs(Sema &S, Decl *D,
                                         const AttributeList &Attr,
                                         SmallVectorImpl<Expr*> &Args,
                                         int Sidx = 0,
                                         bool ParamIdxOk = false) {
  for(unsigned Idx = Sidx; Idx < Attr.getNumArgs(); ++Idx) {
    Expr *ArgExp = Attr.getArgAsExpr(Idx);

    if (ArgExp->isTypeDependent()) {
      // FIXME -- need to check this again on template instantiation
      Args.push_back(ArgExp);
      continue;
    }

    if (StringLiteral *StrLit = dyn_cast<StringLiteral>(ArgExp)) {
      if (StrLit->getLength() == 0 ||
          (StrLit->isAscii() && StrLit->getString() == StringRef("*"))) {
        // Pass empty strings to the analyzer without warnings.
        // Treat "*" as the universal lock.
        Args.push_back(ArgExp);
        continue;
      }

      // We allow constant strings to be used as a placeholder for expressions
      // that are not valid C++ syntax, but warn that they are ignored.
      S.Diag(Attr.getLoc(), diag::warn_thread_attribute_ignored) <<
        Attr.getName();
      Args.push_back(ArgExp);
      continue;
    }

    QualType ArgTy = ArgExp->getType();

    // A pointer to member expression of the form  &MyClass::mu is treated
    // specially -- we need to look at the type of the member.
    if (UnaryOperator *UOp = dyn_cast<UnaryOperator>(ArgExp))
      if (UOp->getOpcode() == UO_AddrOf)
        if (DeclRefExpr *DRE = dyn_cast<DeclRefExpr>(UOp->getSubExpr()))
          if (DRE->getDecl()->isCXXInstanceMember())
            ArgTy = DRE->getDecl()->getType();

    // First see if we can just cast to record type, or point to record type.
    const RecordType *RT = getRecordType(ArgTy);

    // Now check if we index into a record type function param.
    if(!RT && ParamIdxOk) {
      FunctionDecl *FD = dyn_cast<FunctionDecl>(D);
      IntegerLiteral *IL = dyn_cast<IntegerLiteral>(ArgExp);
      if(FD && IL) {
        unsigned int NumParams = FD->getNumParams();
        llvm::APInt ArgValue = IL->getValue();
        uint64_t ParamIdxFromOne = ArgValue.getZExtValue();
        uint64_t ParamIdxFromZero = ParamIdxFromOne - 1;
        if(!ArgValue.isStrictlyPositive() || ParamIdxFromOne > NumParams) {
          S.Diag(Attr.getLoc(), diag::err_attribute_argument_out_of_range)
            << Attr.getName() << Idx + 1 << NumParams;
          continue;
        }
        ArgTy = FD->getParamDecl(ParamIdxFromZero)->getType();
      }
    }

    checkForLockableRecord(S, D, Attr, ArgTy);

    Args.push_back(ArgExp);
  }
}

//===----------------------------------------------------------------------===//
// Attribute Implementations
//===----------------------------------------------------------------------===//

// FIXME: All this manual attribute parsing code is gross. At the
// least add some helper functions to check most argument patterns (#
// and types of args).

static void handlePtGuardedVarAttr(Sema &S, Decl *D,
                                   const AttributeList &Attr) {
  if (!threadSafetyCheckIsPointer(S, D, Attr))
    return;

  D->addAttr(::new (S.Context)
             PtGuardedVarAttr(Attr.getRange(), S.Context,
                              Attr.getAttributeSpellingListIndex()));
}

static bool checkGuardedByAttrCommon(Sema &S, Decl *D,
                                     const AttributeList &Attr,
                                     Expr* &Arg) {
  SmallVector<Expr*, 1> Args;
  // check that all arguments are lockable objects
  checkAttrArgsAreLockableObjs(S, D, Attr, Args);
  unsigned Size = Args.size();
  if (Size != 1)
    return false;

  Arg = Args[0];

  return true;
}

static void handleGuardedByAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  Expr *Arg = 0;
  if (!checkGuardedByAttrCommon(S, D, Attr, Arg))
    return;

  D->addAttr(::new (S.Context) GuardedByAttr(Attr.getRange(), S.Context, Arg));
}

static void handlePtGuardedByAttr(Sema &S, Decl *D,
                                  const AttributeList &Attr) {
  Expr *Arg = 0;
  if (!checkGuardedByAttrCommon(S, D, Attr, Arg))
    return;

  if (!threadSafetyCheckIsPointer(S, D, Attr))
    return;

  D->addAttr(::new (S.Context) PtGuardedByAttr(Attr.getRange(),
                                               S.Context, Arg));
}

static bool checkAcquireOrderAttrCommon(Sema &S, Decl *D,
                                        const AttributeList &Attr,
                                        SmallVectorImpl<Expr *> &Args) {
  if (!checkAttributeAtLeastNumArgs(S, Attr, 1))
    return false;

  // Check that this attribute only applies to lockable types.
  QualType QT = cast<ValueDecl>(D)->getType();
  if (!QT->isDependentType()) {
    const RecordType *RT = getRecordType(QT);
    if (!RT || !RT->getDecl()->getAttr<LockableAttr>()) {
      S.Diag(Attr.getLoc(), diag::warn_thread_attribute_decl_not_lockable)
        << Attr.getName();
      return false;
    }
  }

  // Check that all arguments are lockable objects.
  checkAttrArgsAreLockableObjs(S, D, Attr, Args);
  if (Args.empty())
    return false;

  return true;
}

static void handleAcquiredAfterAttr(Sema &S, Decl *D,
                                    const AttributeList &Attr) {
  SmallVector<Expr*, 1> Args;
  if (!checkAcquireOrderAttrCommon(S, D, Attr, Args))
    return;

  Expr **StartArg = &Args[0];
  D->addAttr(::new (S.Context)
             AcquiredAfterAttr(Attr.getRange(), S.Context,
                               StartArg, Args.size(),
                               Attr.getAttributeSpellingListIndex()));
}

static void handleAcquiredBeforeAttr(Sema &S, Decl *D,
                                     const AttributeList &Attr) {
  SmallVector<Expr*, 1> Args;
  if (!checkAcquireOrderAttrCommon(S, D, Attr, Args))
    return;

  Expr **StartArg = &Args[0];
  D->addAttr(::new (S.Context)
             AcquiredBeforeAttr(Attr.getRange(), S.Context,
                                StartArg, Args.size(),
                                Attr.getAttributeSpellingListIndex()));
}

static bool checkLockFunAttrCommon(Sema &S, Decl *D,
                                   const AttributeList &Attr,
                                   SmallVectorImpl<Expr *> &Args) {
  // zero or more arguments ok
  // check that all arguments are lockable objects
  checkAttrArgsAreLockableObjs(S, D, Attr, Args, 0, /*ParamIdxOk=*/true);

  return true;
}

static void handleSharedLockFunctionAttr(Sema &S, Decl *D,
                                         const AttributeList &Attr) {
  SmallVector<Expr*, 1> Args;
  if (!checkLockFunAttrCommon(S, D, Attr, Args))
    return;

  unsigned Size = Args.size();
  Expr **StartArg = Size == 0 ? 0 : &Args[0];
  D->addAttr(::new (S.Context)
             SharedLockFunctionAttr(Attr.getRange(), S.Context, StartArg, Size,
                                    Attr.getAttributeSpellingListIndex()));
}

static void handleExclusiveLockFunctionAttr(Sema &S, Decl *D,
                                            const AttributeList &Attr) {
  SmallVector<Expr*, 1> Args;
  if (!checkLockFunAttrCommon(S, D, Attr, Args))
    return;

  unsigned Size = Args.size();
  Expr **StartArg = Size == 0 ? 0 : &Args[0];
  D->addAttr(::new (S.Context)
             ExclusiveLockFunctionAttr(Attr.getRange(), S.Context,
                                       StartArg, Size,
                                       Attr.getAttributeSpellingListIndex()));
}

static void handleAssertSharedLockAttr(Sema &S, Decl *D,
                                       const AttributeList &Attr) {
  SmallVector<Expr*, 1> Args;
  if (!checkLockFunAttrCommon(S, D, Attr, Args))
    return;

  unsigned Size = Args.size();
  Expr **StartArg = Size == 0 ? 0 : &Args[0];
  D->addAttr(::new (S.Context)
             AssertSharedLockAttr(Attr.getRange(), S.Context, StartArg, Size,
                                  Attr.getAttributeSpellingListIndex()));
}

static void handleAssertExclusiveLockAttr(Sema &S, Decl *D,
                                          const AttributeList &Attr) {
  SmallVector<Expr*, 1> Args;
  if (!checkLockFunAttrCommon(S, D, Attr, Args))
    return;

  unsigned Size = Args.size();
  Expr **StartArg = Size == 0 ? 0 : &Args[0];
  D->addAttr(::new (S.Context)
             AssertExclusiveLockAttr(Attr.getRange(), S.Context,
                                     StartArg, Size,
                                     Attr.getAttributeSpellingListIndex()));
}


static bool checkTryLockFunAttrCommon(Sema &S, Decl *D,
                                      const AttributeList &Attr,
                                      SmallVectorImpl<Expr *> &Args) {
  if (!checkAttributeAtLeastNumArgs(S, Attr, 1))
    return false;

  if (!isIntOrBool(Attr.getArgAsExpr(0))) {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_n_type)
      << Attr.getName() << 1 << AANT_ArgumentIntOrBool;
    return false;
  }

  // check that all arguments are lockable objects
  checkAttrArgsAreLockableObjs(S, D, Attr, Args, 1);

  return true;
}

static void handleSharedTrylockFunctionAttr(Sema &S, Decl *D,
                                            const AttributeList &Attr) {
  SmallVector<Expr*, 2> Args;
  if (!checkTryLockFunAttrCommon(S, D, Attr, Args))
    return;

  D->addAttr(::new (S.Context)
             SharedTrylockFunctionAttr(Attr.getRange(), S.Context,
                                       Attr.getArgAsExpr(0),
                                       Args.data(), Args.size(),
                                       Attr.getAttributeSpellingListIndex()));
}

static void handleExclusiveTrylockFunctionAttr(Sema &S, Decl *D,
                                               const AttributeList &Attr) {
  SmallVector<Expr*, 2> Args;
  if (!checkTryLockFunAttrCommon(S, D, Attr, Args))
    return;

  D->addAttr(::new (S.Context)
             ExclusiveTrylockFunctionAttr(Attr.getRange(), S.Context,
                                          Attr.getArgAsExpr(0),
                                          Args.data(), Args.size(),
                                          Attr.getAttributeSpellingListIndex()));
}

static bool checkLocksRequiredCommon(Sema &S, Decl *D,
                                     const AttributeList &Attr,
                                     SmallVectorImpl<Expr *> &Args) {
  if (!checkAttributeAtLeastNumArgs(S, Attr, 1))
    return false;

  // check that all arguments are lockable objects
  checkAttrArgsAreLockableObjs(S, D, Attr, Args);
  if (Args.empty())
    return false;

  return true;
}

static void handleExclusiveLocksRequiredAttr(Sema &S, Decl *D,
                                             const AttributeList &Attr) {
  SmallVector<Expr*, 1> Args;
  if (!checkLocksRequiredCommon(S, D, Attr, Args))
    return;

  Expr **StartArg = &Args[0];
  D->addAttr(::new (S.Context)
             ExclusiveLocksRequiredAttr(Attr.getRange(), S.Context,
                                        StartArg, Args.size(),
                                        Attr.getAttributeSpellingListIndex()));
}

static void handleSharedLocksRequiredAttr(Sema &S, Decl *D,
                                          const AttributeList &Attr) {
  SmallVector<Expr*, 1> Args;
  if (!checkLocksRequiredCommon(S, D, Attr, Args))
    return;

  Expr **StartArg = &Args[0];
  D->addAttr(::new (S.Context)
             SharedLocksRequiredAttr(Attr.getRange(), S.Context,
                                     StartArg, Args.size(),
                                     Attr.getAttributeSpellingListIndex()));
}

static void handleUnlockFunAttr(Sema &S, Decl *D,
                                const AttributeList &Attr) {
  // zero or more arguments ok
  // check that all arguments are lockable objects
  SmallVector<Expr*, 1> Args;
  checkAttrArgsAreLockableObjs(S, D, Attr, Args, 0, /*ParamIdxOk=*/true);
  unsigned Size = Args.size();
  Expr **StartArg = Size == 0 ? 0 : &Args[0];

  D->addAttr(::new (S.Context)
             UnlockFunctionAttr(Attr.getRange(), S.Context, StartArg, Size,
                                Attr.getAttributeSpellingListIndex()));
}

static void handleLockReturnedAttr(Sema &S, Decl *D,
                                   const AttributeList &Attr) {
  // check that the argument is lockable object
  SmallVector<Expr*, 1> Args;
  checkAttrArgsAreLockableObjs(S, D, Attr, Args);
  unsigned Size = Args.size();
  if (Size == 0)
    return;

  D->addAttr(::new (S.Context)
             LockReturnedAttr(Attr.getRange(), S.Context, Args[0],
                              Attr.getAttributeSpellingListIndex()));
}

static void handleLocksExcludedAttr(Sema &S, Decl *D,
                                    const AttributeList &Attr) {
  if (!checkAttributeAtLeastNumArgs(S, Attr, 1))
    return;

  // check that all arguments are lockable objects
  SmallVector<Expr*, 1> Args;
  checkAttrArgsAreLockableObjs(S, D, Attr, Args);
  unsigned Size = Args.size();
  if (Size == 0)
    return;
  Expr **StartArg = &Args[0];

  D->addAttr(::new (S.Context)
             LocksExcludedAttr(Attr.getRange(), S.Context, StartArg, Size,
                               Attr.getAttributeSpellingListIndex()));
}

static void handleConsumableAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  ConsumableAttr::ConsumedState DefaultState;

  if (Attr.isArgIdent(0)) {
    IdentifierLoc *IL = Attr.getArgAsIdent(0);
    if (!ConsumableAttr::ConvertStrToConsumedState(IL->Ident->getName(),
                                                   DefaultState)) {
      S.Diag(IL->Loc, diag::warn_attribute_type_not_supported)
        << Attr.getName() << IL->Ident;
      return;
    }
  } else {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_type)
        << Attr.getName() << AANT_ArgumentIdentifier;
    return;
  }
  
  D->addAttr(::new (S.Context)
             ConsumableAttr(Attr.getRange(), S.Context, DefaultState,
                            Attr.getAttributeSpellingListIndex()));
}

static bool checkForConsumableClass(Sema &S, const CXXMethodDecl *MD,
                                        const AttributeList &Attr) {
  ASTContext &CurrContext = S.getASTContext();
  QualType ThisType = MD->getThisType(CurrContext)->getPointeeType();
  
  if (const CXXRecordDecl *RD = ThisType->getAsCXXRecordDecl()) {
    if (!RD->hasAttr<ConsumableAttr>()) {
      S.Diag(Attr.getLoc(), diag::warn_attr_on_unconsumable_class) <<
        RD->getNameAsString();
      
      return false;
    }
  }
  
  return true;
}


static void handleCallableWhenAttr(Sema &S, Decl *D,
                                   const AttributeList &Attr) {
  if (!checkAttributeAtLeastNumArgs(S, Attr, 1))
    return;
  
  if (!checkForConsumableClass(S, cast<CXXMethodDecl>(D), Attr))
    return;
  
  SmallVector<CallableWhenAttr::ConsumedState, 3> States;
  for (unsigned ArgIndex = 0; ArgIndex < Attr.getNumArgs(); ++ArgIndex) {
    CallableWhenAttr::ConsumedState CallableState;
    
    StringRef StateString;
    SourceLocation Loc;
    if (!S.checkStringLiteralArgumentAttr(Attr, ArgIndex, StateString, &Loc))
      return;

    if (!CallableWhenAttr::ConvertStrToConsumedState(StateString,
                                                     CallableState)) {
      S.Diag(Loc, diag::warn_attribute_type_not_supported)
        << Attr.getName() << StateString;
      return;
    }
      
    States.push_back(CallableState);
  }
  
  D->addAttr(::new (S.Context)
             CallableWhenAttr(Attr.getRange(), S.Context, States.data(),
               States.size(), Attr.getAttributeSpellingListIndex()));
}


static void handleParamTypestateAttr(Sema &S, Decl *D,
                                    const AttributeList &Attr) {
  if (!checkAttributeNumArgs(S, Attr, 1)) return;
    
  ParamTypestateAttr::ConsumedState ParamState;
  
  if (Attr.isArgIdent(0)) {
    IdentifierLoc *Ident = Attr.getArgAsIdent(0);
    StringRef StateString = Ident->Ident->getName();

    if (!ParamTypestateAttr::ConvertStrToConsumedState(StateString,
                                                       ParamState)) {
      S.Diag(Ident->Loc, diag::warn_attribute_type_not_supported)
        << Attr.getName() << StateString;
      return;
    }
  } else {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_type) <<
      Attr.getName() << AANT_ArgumentIdentifier;
    return;
  }
  
  // FIXME: This check is currently being done in the analysis.  It can be
  //        enabled here only after the parser propagates attributes at
  //        template specialization definition, not declaration.
  //QualType ReturnType = cast<ParmVarDecl>(D)->getType();
  //const CXXRecordDecl *RD = ReturnType->getAsCXXRecordDecl();
  //
  //if (!RD || !RD->hasAttr<ConsumableAttr>()) {
  //    S.Diag(Attr.getLoc(), diag::warn_return_state_for_unconsumable_type) <<
  //      ReturnType.getAsString();
  //    return;
  //}
  
  D->addAttr(::new (S.Context)
             ParamTypestateAttr(Attr.getRange(), S.Context, ParamState,
                                Attr.getAttributeSpellingListIndex()));
}


static void handleReturnTypestateAttr(Sema &S, Decl *D,
                                      const AttributeList &Attr) {
  if (!checkAttributeNumArgs(S, Attr, 1)) return;
  
  ReturnTypestateAttr::ConsumedState ReturnState;
  
  if (Attr.isArgIdent(0)) {
    IdentifierLoc *IL = Attr.getArgAsIdent(0);
    if (!ReturnTypestateAttr::ConvertStrToConsumedState(IL->Ident->getName(),
                                                        ReturnState)) {
      S.Diag(IL->Loc, diag::warn_attribute_type_not_supported)
        << Attr.getName() << IL->Ident;
      return;
    }
  } else {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_type) <<
      Attr.getName() << AANT_ArgumentIdentifier;
    return;
  }
  
  // FIXME: This check is currently being done in the analysis.  It can be
  //        enabled here only after the parser propagates attributes at
  //        template specialization definition, not declaration.
  //QualType ReturnType;
  //
  //if (const ParmVarDecl *Param = dyn_cast<ParmVarDecl>(D)) {
  //  ReturnType = Param->getType();
  //
  //} else if (const CXXConstructorDecl *Constructor =
  //             dyn_cast<CXXConstructorDecl>(D)) {
  //  ReturnType = Constructor->getThisType(S.getASTContext())->getPointeeType();
  //  
  //} else {
  //  
  //  ReturnType = cast<FunctionDecl>(D)->getCallResultType();
  //}
  //
  //const CXXRecordDecl *RD = ReturnType->getAsCXXRecordDecl();
  //
  //if (!RD || !RD->hasAttr<ConsumableAttr>()) {
  //    S.Diag(Attr.getLoc(), diag::warn_return_state_for_unconsumable_type) <<
  //      ReturnType.getAsString();
  //    return;
  //}
  
  D->addAttr(::new (S.Context)
             ReturnTypestateAttr(Attr.getRange(), S.Context, ReturnState,
                                 Attr.getAttributeSpellingListIndex()));
}


static void handleSetTypestateAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (!checkAttributeNumArgs(S, Attr, 1))
    return;
  
  if (!checkForConsumableClass(S, cast<CXXMethodDecl>(D), Attr))
    return;
  
  SetTypestateAttr::ConsumedState NewState;
  if (Attr.isArgIdent(0)) {
    IdentifierLoc *Ident = Attr.getArgAsIdent(0);
    StringRef Param = Ident->Ident->getName();
    if (!SetTypestateAttr::ConvertStrToConsumedState(Param, NewState)) {
      S.Diag(Ident->Loc, diag::warn_attribute_type_not_supported)
        << Attr.getName() << Param;
      return;
    }
  } else {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_type) <<
      Attr.getName() << AANT_ArgumentIdentifier;
    return;
  }
  
  D->addAttr(::new (S.Context)
             SetTypestateAttr(Attr.getRange(), S.Context, NewState,
                              Attr.getAttributeSpellingListIndex()));
}

static void handleTestTypestateAttr(Sema &S, Decl *D,
                                    const AttributeList &Attr) {
  if (!checkAttributeNumArgs(S, Attr, 1))
    return;
  
  if (!checkForConsumableClass(S, cast<CXXMethodDecl>(D), Attr))
    return;
  
  TestTypestateAttr::ConsumedState TestState;  
  if (Attr.isArgIdent(0)) {
    IdentifierLoc *Ident = Attr.getArgAsIdent(0);
    StringRef Param = Ident->Ident->getName();
    if (!TestTypestateAttr::ConvertStrToConsumedState(Param, TestState)) {
      S.Diag(Ident->Loc, diag::warn_attribute_type_not_supported)
        << Attr.getName() << Param;
      return;
    }
  } else {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_type) <<
      Attr.getName() << AANT_ArgumentIdentifier;
    return;
  }
  
  D->addAttr(::new (S.Context)
             TestTypestateAttr(Attr.getRange(), S.Context, TestState,
                                Attr.getAttributeSpellingListIndex()));
}

static void handleExtVectorTypeAttr(Sema &S, Scope *scope, Decl *D,
                                    const AttributeList &Attr) {
  // Remember this typedef decl, we will need it later for diagnostics.
  S.ExtVectorDecls.push_back(cast<TypedefNameDecl>(D));
}

static void handlePackedAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (TagDecl *TD = dyn_cast<TagDecl>(D))
    TD->addAttr(::new (S.Context) PackedAttr(Attr.getRange(), S.Context));
  else if (FieldDecl *FD = dyn_cast<FieldDecl>(D)) {
    // If the alignment is less than or equal to 8 bits, the packed attribute
    // has no effect.
    if (!FD->getType()->isDependentType() &&
        !FD->getType()->isIncompleteType() &&
        S.Context.getTypeAlign(FD->getType()) <= 8)
      S.Diag(Attr.getLoc(), diag::warn_attribute_ignored_for_field_of_type)
        << Attr.getName() << FD->getType();
    else
      FD->addAttr(::new (S.Context)
                  PackedAttr(Attr.getRange(), S.Context,
                             Attr.getAttributeSpellingListIndex()));
  } else
    S.Diag(Attr.getLoc(), diag::warn_attribute_ignored) << Attr.getName();
}

static bool checkIBOutletCommon(Sema &S, Decl *D, const AttributeList &Attr) {
  // The IBOutlet/IBOutletCollection attributes only apply to instance
  // variables or properties of Objective-C classes.  The outlet must also
  // have an object reference type.
  if (const ObjCIvarDecl *VD = dyn_cast<ObjCIvarDecl>(D)) {
    if (!VD->getType()->getAs<ObjCObjectPointerType>()) {
      S.Diag(Attr.getLoc(), diag::warn_iboutlet_object_type)
        << Attr.getName() << VD->getType() << 0;
      return false;
    }
  }
  else if (const ObjCPropertyDecl *PD = dyn_cast<ObjCPropertyDecl>(D)) {
    if (!PD->getType()->getAs<ObjCObjectPointerType>()) {
      S.Diag(Attr.getLoc(), diag::warn_iboutlet_object_type)
        << Attr.getName() << PD->getType() << 1;
      return false;
    }
  }
  else {
    S.Diag(Attr.getLoc(), diag::warn_attribute_iboutlet) << Attr.getName();
    return false;
  }

  return true;
}

static void handleIBOutlet(Sema &S, Decl *D, const AttributeList &Attr) {
  if (!checkIBOutletCommon(S, D, Attr))
    return;

  D->addAttr(::new (S.Context)
             IBOutletAttr(Attr.getRange(), S.Context,
                          Attr.getAttributeSpellingListIndex()));
}

static void handleIBOutletCollection(Sema &S, Decl *D,
                                     const AttributeList &Attr) {

  // The iboutletcollection attribute can have zero or one arguments.
  if (Attr.getNumArgs() > 1) {
    S.Diag(Attr.getLoc(), diag::err_attribute_wrong_number_arguments)
      << Attr.getName() << 1;
    return;
  }

  if (!checkIBOutletCommon(S, D, Attr))
    return;

  ParsedType PT;

  if (Attr.hasParsedType())
    PT = Attr.getTypeArg();
  else {
    PT = S.getTypeName(S.Context.Idents.get("NSObject"), Attr.getLoc(),
                       S.getScopeForContext(D->getDeclContext()->getParent()));
    if (!PT) {
      S.Diag(Attr.getLoc(), diag::err_iboutletcollection_type) << "NSObject";
      return;
    }
  }

  TypeSourceInfo *QTLoc = 0;
  QualType QT = S.GetTypeFromParser(PT, &QTLoc);
  if (!QTLoc)
    QTLoc = S.Context.getTrivialTypeSourceInfo(QT, Attr.getLoc());

  // Diagnose use of non-object type in iboutletcollection attribute.
  // FIXME. Gnu attribute extension ignores use of builtin types in
  // attributes. So, __attribute__((iboutletcollection(char))) will be
  // treated as __attribute__((iboutletcollection())).
  if (!QT->isObjCIdType() && !QT->isObjCObjectType()) {
    S.Diag(Attr.getLoc(),
           QT->isBuiltinType() ? diag::err_iboutletcollection_builtintype
                               : diag::err_iboutletcollection_type) << QT;
    return;
  }

  D->addAttr(::new (S.Context)
             IBOutletCollectionAttr(Attr.getRange(), S.Context, QTLoc,
                                    Attr.getAttributeSpellingListIndex()));
}

static void possibleTransparentUnionPointerType(QualType &T) {
  if (const RecordType *UT = T->getAsUnionType())
    if (UT && UT->getDecl()->hasAttr<TransparentUnionAttr>()) {
      RecordDecl *UD = UT->getDecl();
      for (RecordDecl::field_iterator it = UD->field_begin(),
           itend = UD->field_end(); it != itend; ++it) {
        QualType QT = it->getType();
        if (QT->isAnyPointerType() || QT->isBlockPointerType()) {
          T = QT;
          return;
        }
      }
    }
}

static void handleAllocSizeAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (!isFunctionOrMethod(D)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
    << Attr.getName() << ExpectedFunctionOrMethod;
    return;
  }

  if (!checkAttributeAtLeastNumArgs(S, Attr, 1))
    return;

  SmallVector<unsigned, 8> SizeArgs;
  for (unsigned i = 0; i < Attr.getNumArgs(); ++i) {
    Expr *Ex = Attr.getArgAsExpr(i);
    uint64_t Idx;
    if (!checkFunctionOrMethodArgumentIndex(S, D, Attr.getName()->getName(),
                                            Attr.getLoc(), i + 1, Ex, Idx))
      return;

    // check if the function argument is of an integer type
    QualType T = getFunctionOrMethodArgType(D, Idx).getNonReferenceType();
    if (!T->isIntegerType()) {
      S.Diag(Attr.getLoc(), diag::err_attribute_argument_type)
        << Attr.getName() << AANT_ArgumentIntegerConstant
        << Ex->getSourceRange();
      return;
    }
    SizeArgs.push_back(Idx);
  }

  // check if the function returns a pointer
  if (!getFunctionType(D)->getResultType()->isAnyPointerType()) {
    S.Diag(Attr.getLoc(), diag::warn_ns_attribute_wrong_return_type)
    << Attr.getName() << 0 /*function*/<< 1 /*pointer*/ << D->getSourceRange();
  }

  D->addAttr(::new (S.Context)
             AllocSizeAttr(Attr.getRange(), S.Context,
                           SizeArgs.data(), SizeArgs.size(),
                           Attr.getAttributeSpellingListIndex()));
}

static void handleNonNullAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  // GCC ignores the nonnull attribute on K&R style function prototypes, so we
  // ignore it as well
  if (!isFunctionOrMethod(D) || !hasFunctionProto(D)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedFunction;
    return;
  }

  SmallVector<unsigned, 8> NonNullArgs;
  for (unsigned i = 0; i < Attr.getNumArgs(); ++i) {
    Expr *Ex = Attr.getArgAsExpr(i);
    uint64_t Idx;
    if (!checkFunctionOrMethodArgumentIndex(S, D, Attr.getName()->getName(),
                                            Attr.getLoc(), i + 1, Ex, Idx))
      return;

    // Is the function argument a pointer type?
    QualType T = getFunctionOrMethodArgType(D, Idx).getNonReferenceType();
    possibleTransparentUnionPointerType(T);
    
    if (!T->isAnyPointerType() && !T->isBlockPointerType()) {
      // FIXME: Should also highlight argument in decl.
      S.Diag(Attr.getLoc(), diag::warn_nonnull_pointers_only)
        << "nonnull" << Ex->getSourceRange();
      continue;
    }

    NonNullArgs.push_back(Idx);
  }

  // If no arguments were specified to __attribute__((nonnull)) then all pointer
  // arguments have a nonnull attribute.
  if (NonNullArgs.empty()) {
    for (unsigned i = 0, e = getFunctionOrMethodNumArgs(D); i != e; ++i) {
      QualType T = getFunctionOrMethodArgType(D, i).getNonReferenceType();
      possibleTransparentUnionPointerType(T);
      if (T->isAnyPointerType() || T->isBlockPointerType())
        NonNullArgs.push_back(i);
    }

    // No pointer arguments?
    if (NonNullArgs.empty()) {
      // Warn the trivial case only if attribute is not coming from a
      // macro instantiation.
      if (Attr.getLoc().isFileID())
        S.Diag(Attr.getLoc(), diag::warn_attribute_nonnull_no_pointers);
      return;
    }
  }

  unsigned *start = &NonNullArgs[0];
  unsigned size = NonNullArgs.size();
  llvm::array_pod_sort(start, start + size);
  D->addAttr(::new (S.Context)
             NonNullAttr(Attr.getRange(), S.Context, start, size,
                         Attr.getAttributeSpellingListIndex()));
}

static const char *ownershipKindToDiagName(OwnershipAttr::OwnershipKind K) {
  switch (K) {
    case OwnershipAttr::Holds:    return "'ownership_holds'";
    case OwnershipAttr::Takes:    return "'ownership_takes'";
    case OwnershipAttr::Returns:  return "'ownership_returns'";
  }
  llvm_unreachable("unknown ownership");
}

static void handleOwnershipAttr(Sema &S, Decl *D, const AttributeList &AL) {
  // This attribute must be applied to a function declaration. The first
  // argument to the attribute must be an identifier, the name of the resource,
  // for example: malloc. The following arguments must be argument indexes, the
  // arguments must be of integer type for Returns, otherwise of pointer type.
  // The difference between Holds and Takes is that a pointer may still be used
  // after being held. free() should be __attribute((ownership_takes)), whereas
  // a list append function may well be __attribute((ownership_holds)).

  if (!AL.isArgIdent(0)) {
    S.Diag(AL.getLoc(), diag::err_attribute_argument_n_type)
      << AL.getName() << 1 << AANT_ArgumentIdentifier;
    return;
  }

  // Figure out our Kind.
  OwnershipAttr::OwnershipKind K =
      OwnershipAttr(AL.getLoc(), S.Context, 0, 0, 0,
                    AL.getAttributeSpellingListIndex()).getOwnKind();

  // Check arguments.
  switch (K) {
  case OwnershipAttr::Takes:
  case OwnershipAttr::Holds:
    if (AL.getNumArgs() < 2) {
      S.Diag(AL.getLoc(), diag::err_attribute_too_few_arguments) << 2;
      return;
    }
    break;
  case OwnershipAttr::Returns:
    if (AL.getNumArgs() > 2) {
      S.Diag(AL.getLoc(), diag::err_attribute_too_many_arguments) << 1;
      return;
    }
    break;
  }

  if (!isFunction(D) || !hasFunctionProto(D)) {
    S.Diag(AL.getLoc(), diag::warn_attribute_wrong_decl_type)
      << AL.getName() << ExpectedFunction;
    return;
  }

  IdentifierInfo *Module = AL.getArgAsIdent(0)->Ident;

  // Normalize the argument, __foo__ becomes foo.
  StringRef ModuleName = Module->getName();
  if (ModuleName.startswith("__") && ModuleName.endswith("__") &&
      ModuleName.size() > 4) {
    ModuleName = ModuleName.drop_front(2).drop_back(2);
    Module = &S.PP.getIdentifierTable().get(ModuleName);
  }

  SmallVector<unsigned, 8> OwnershipArgs;
  for (unsigned i = 1; i < AL.getNumArgs(); ++i) {
    Expr *Ex = AL.getArgAsExpr(i);
    uint64_t Idx;
    if (!checkFunctionOrMethodArgumentIndex(S, D, AL.getName()->getName(),
                                            AL.getLoc(), i, Ex, Idx))
      return;

    // Is the function argument a pointer type?
    QualType T = getFunctionOrMethodArgType(D, Idx);
    int Err = -1;  // No error
    switch (K) {
      case OwnershipAttr::Takes:
      case OwnershipAttr::Holds:
        if (!T->isAnyPointerType() && !T->isBlockPointerType())
          Err = 0;
        break;
      case OwnershipAttr::Returns:
        if (!T->isIntegerType())
          Err = 1;
        break;
    }
    if (-1 != Err) {
      S.Diag(AL.getLoc(), diag::err_ownership_type) << AL.getName() << Err
        << Ex->getSourceRange();
      return;
    }

    // Check we don't have a conflict with another ownership attribute.
    for (specific_attr_iterator<OwnershipAttr>
         i = D->specific_attr_begin<OwnershipAttr>(),
         e = D->specific_attr_end<OwnershipAttr>(); i != e; ++i) {
      // FIXME: A returns attribute should conflict with any returns attribute
      // with a different index too.
      if ((*i)->getOwnKind() != K && (*i)->args_end() !=
          std::find((*i)->args_begin(), (*i)->args_end(), Idx)) {
        S.Diag(AL.getLoc(), diag::err_attributes_are_not_compatible)
          << AL.getName() << ownershipKindToDiagName((*i)->getOwnKind());
        return;
      }
    }
    OwnershipArgs.push_back(Idx);
  }

  unsigned* start = OwnershipArgs.data();
  unsigned size = OwnershipArgs.size();
  llvm::array_pod_sort(start, start + size);

  D->addAttr(::new (S.Context)
             OwnershipAttr(AL.getLoc(), S.Context, Module, start, size,
                           AL.getAttributeSpellingListIndex()));
}

static void handleWeakRefAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  // Check the attribute arguments.
  if (Attr.getNumArgs() > 1) {
    S.Diag(Attr.getLoc(), diag::err_attribute_wrong_number_arguments)
      << Attr.getName() << 1;
    return;
  }

  NamedDecl *nd = cast<NamedDecl>(D);

  // gcc rejects
  // class c {
  //   static int a __attribute__((weakref ("v2")));
  //   static int b() __attribute__((weakref ("f3")));
  // };
  // and ignores the attributes of
  // void f(void) {
  //   static int a __attribute__((weakref ("v2")));
  // }
  // we reject them
  const DeclContext *Ctx = D->getDeclContext()->getRedeclContext();
  if (!Ctx->isFileContext()) {
    S.Diag(Attr.getLoc(), diag::err_attribute_weakref_not_global_context) <<
        nd->getNameAsString();
    return;
  }

  // The GCC manual says
  //
  // At present, a declaration to which `weakref' is attached can only
  // be `static'.
  //
  // It also says
  //
  // Without a TARGET,
  // given as an argument to `weakref' or to `alias', `weakref' is
  // equivalent to `weak'.
  //
  // gcc 4.4.1 will accept
  // int a7 __attribute__((weakref));
  // as
  // int a7 __attribute__((weak));
  // This looks like a bug in gcc. We reject that for now. We should revisit
  // it if this behaviour is actually used.

  // GCC rejects
  // static ((alias ("y"), weakref)).
  // Should we? How to check that weakref is before or after alias?

  // FIXME: it would be good for us to keep the WeakRefAttr as-written instead
  // of transforming it into an AliasAttr.  The WeakRefAttr never uses the
  // StringRef parameter it was given anyway.
  StringRef Str;
  if (Attr.getNumArgs() && S.checkStringLiteralArgumentAttr(Attr, 0, Str))
    // GCC will accept anything as the argument of weakref. Should we
    // check for an existing decl?
    D->addAttr(::new (S.Context) AliasAttr(Attr.getRange(), S.Context, Str,
                                        Attr.getAttributeSpellingListIndex()));

  D->addAttr(::new (S.Context)
             WeakRefAttr(Attr.getRange(), S.Context,
                         Attr.getAttributeSpellingListIndex()));
}

static void handleAliasAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  StringRef Str;
  if (!S.checkStringLiteralArgumentAttr(Attr, 0, Str))
    return;

  if (S.Context.getTargetInfo().getTriple().isOSDarwin()) {
    S.Diag(Attr.getLoc(), diag::err_alias_not_supported_on_darwin);
    return;
  }

  // FIXME: check if target symbol exists in current file

  D->addAttr(::new (S.Context) AliasAttr(Attr.getRange(), S.Context, Str,
                                         Attr.getAttributeSpellingListIndex()));
}

static void handleColdAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (checkAttrMutualExclusion<HotAttr>(S, D, Attr, "hot"))
    return;

  D->addAttr(::new (S.Context) ColdAttr(Attr.getRange(), S.Context,
                                        Attr.getAttributeSpellingListIndex()));
}

static void handleHotAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (checkAttrMutualExclusion<ColdAttr>(S, D, Attr, "cold"))
    return;

  D->addAttr(::new (S.Context) HotAttr(Attr.getRange(), S.Context,
                                       Attr.getAttributeSpellingListIndex()));
}

static void handleTLSModelAttr(Sema &S, Decl *D,
                               const AttributeList &Attr) {
  StringRef Model;
  SourceLocation LiteralLoc;
  // Check that it is a string.
  if (!S.checkStringLiteralArgumentAttr(Attr, 0, Model, &LiteralLoc))
    return;

  // Check that the value.
  if (Model != "global-dynamic" && Model != "local-dynamic"
      && Model != "initial-exec" && Model != "local-exec") {
    S.Diag(LiteralLoc, diag::err_attr_tlsmodel_arg);
    return;
  }

  D->addAttr(::new (S.Context)
             TLSModelAttr(Attr.getRange(), S.Context, Model,
                          Attr.getAttributeSpellingListIndex()));
}

static void handleMallocAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (const FunctionDecl *FD = dyn_cast<FunctionDecl>(D)) {
    QualType RetTy = FD->getResultType();
    if (RetTy->isAnyPointerType() || RetTy->isBlockPointerType()) {
      D->addAttr(::new (S.Context)
                 MallocAttr(Attr.getRange(), S.Context,
                            Attr.getAttributeSpellingListIndex()));
      return;
    }
  }

  S.Diag(Attr.getLoc(), diag::warn_attribute_malloc_pointer_only);
}

static void handleCommonAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (S.LangOpts.CPlusPlus) {
    S.Diag(Attr.getLoc(), diag::err_attribute_not_supported_in_lang)
      << Attr.getName() << AttributeLangSupport::Cpp;
    return;
  }

  D->addAttr(::new (S.Context) CommonAttr(Attr.getRange(), S.Context,
                                        Attr.getAttributeSpellingListIndex()));
}

static void handleNoReturnAttr(Sema &S, Decl *D, const AttributeList &attr) {
  if (hasDeclarator(D)) return;

  if (S.CheckNoReturnAttr(attr)) return;

  if (!isa<ObjCMethodDecl>(D)) {
    S.Diag(attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << attr.getName() << ExpectedFunctionOrMethod;
    return;
  }

  D->addAttr(::new (S.Context)
             NoReturnAttr(attr.getRange(), S.Context,
                          attr.getAttributeSpellingListIndex()));
}

bool Sema::CheckNoReturnAttr(const AttributeList &attr) {
  if (!checkAttributeNumArgs(*this, attr, 0)) {
    attr.setInvalid();
    return true;
  }

  return false;
}

static void handleAnalyzerNoReturnAttr(Sema &S, Decl *D,
                                       const AttributeList &Attr) {
  
  // The checking path for 'noreturn' and 'analyzer_noreturn' are different
  // because 'analyzer_noreturn' does not impact the type.
  if (!isFunctionOrMethod(D) && !isa<BlockDecl>(D)) {
    ValueDecl *VD = dyn_cast<ValueDecl>(D);
    if (VD == 0 || (!VD->getType()->isBlockPointerType()
                    && !VD->getType()->isFunctionPointerType())) {
      S.Diag(Attr.getLoc(),
             Attr.isCXX11Attribute() ? diag::err_attribute_wrong_decl_type
             : diag::warn_attribute_wrong_decl_type)
        << Attr.getName() << ExpectedFunctionMethodOrBlock;
      return;
    }
  }
  
  D->addAttr(::new (S.Context)
             AnalyzerNoReturnAttr(Attr.getRange(), S.Context,
                                  Attr.getAttributeSpellingListIndex()));
}

// PS3 PPU-specific.
static void handleVecReturnAttr(Sema &S, Decl *D, const AttributeList &Attr) {
/*
  Returning a Vector Class in Registers
  
  According to the PPU ABI specifications, a class with a single member of 
  vector type is returned in memory when used as the return value of a function.
  This results in inefficient code when implementing vector classes. To return
  the value in a single vector register, add the vecreturn attribute to the
  class definition. This attribute is also applicable to struct types.
  
  Example:
  
  struct Vector
  {
    __vector float xyzw;
  } __attribute__((vecreturn));
  
  Vector Add(Vector lhs, Vector rhs)
  {
    Vector result;
    result.xyzw = vec_add(lhs.xyzw, rhs.xyzw);
    return result; // This will be returned in a register
  }
*/
  if (D->getAttr<VecReturnAttr>()) {
    S.Diag(Attr.getLoc(), diag::err_repeat_attribute) << "vecreturn";
    return;
  }

  RecordDecl *record = cast<RecordDecl>(D);
  int count = 0;

  if (!isa<CXXRecordDecl>(record)) {
    S.Diag(Attr.getLoc(), diag::err_attribute_vecreturn_only_vector_member);
    return;
  }

  if (!cast<CXXRecordDecl>(record)->isPOD()) {
    S.Diag(Attr.getLoc(), diag::err_attribute_vecreturn_only_pod_record);
    return;
  }

  for (RecordDecl::field_iterator iter = record->field_begin();
       iter != record->field_end(); iter++) {
    if ((count == 1) || !iter->getType()->isVectorType()) {
      S.Diag(Attr.getLoc(), diag::err_attribute_vecreturn_only_vector_member);
      return;
    }
    count++;
  }

  D->addAttr(::new (S.Context)
             VecReturnAttr(Attr.getRange(), S.Context,
                           Attr.getAttributeSpellingListIndex()));
}

static void handleDependencyAttr(Sema &S, Scope *Scope, Decl *D,
                                 const AttributeList &Attr) {
  if (isa<ParmVarDecl>(D)) {
    // [[carries_dependency]] can only be applied to a parameter if it is a
    // parameter of a function declaration or lambda.
    if (!(Scope->getFlags() & clang::Scope::FunctionDeclarationScope)) {
      S.Diag(Attr.getLoc(),
             diag::err_carries_dependency_param_not_function_decl);
      return;
    }
  }

  D->addAttr(::new (S.Context) CarriesDependencyAttr(
                                   Attr.getRange(), S.Context,
                                   Attr.getAttributeSpellingListIndex()));
}

static void handleUnusedAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (!isa<VarDecl>(D) && !isa<ObjCIvarDecl>(D) && !isFunctionOrMethod(D) &&
      !isa<TypeDecl>(D) && !isa<LabelDecl>(D) && !isa<FieldDecl>(D)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedVariableFunctionOrLabel;
    return;
  }

  D->addAttr(::new (S.Context)
             UnusedAttr(Attr.getRange(), S.Context,
                        Attr.getAttributeSpellingListIndex()));
}

static void handleUsedAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (const VarDecl *VD = dyn_cast<VarDecl>(D)) {
    if (VD->hasLocalStorage()) {
      S.Diag(Attr.getLoc(), diag::warn_attribute_ignored) << "used";
      return;
    }
  } else if (!isFunctionOrMethod(D)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedVariableOrFunction;
    return;
  }

  D->addAttr(::new (S.Context)
             UsedAttr(Attr.getRange(), S.Context,
                      Attr.getAttributeSpellingListIndex()));
}

static void handleConstructorAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  // check the attribute arguments.
  if (Attr.getNumArgs() > 1) {
    S.Diag(Attr.getLoc(), diag::err_attribute_too_many_arguments) << 1;
    return;
  }

  uint32_t priority = ConstructorAttr::DefaultPriority;
  if (Attr.getNumArgs() > 0 &&
      !checkUInt32Argument(S, Attr, Attr.getArgAsExpr(0), priority))
    return;

  D->addAttr(::new (S.Context)
             ConstructorAttr(Attr.getRange(), S.Context, priority,
                             Attr.getAttributeSpellingListIndex()));
}

static void handleDestructorAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  // check the attribute arguments.
  if (Attr.getNumArgs() > 1) {
    S.Diag(Attr.getLoc(), diag::err_attribute_too_many_arguments) << 1;
    return;
  }

  uint32_t priority = ConstructorAttr::DefaultPriority;
  if (Attr.getNumArgs() > 0 &&
      !checkUInt32Argument(S, Attr, Attr.getArgAsExpr(0), priority))
    return;

  D->addAttr(::new (S.Context)
             DestructorAttr(Attr.getRange(), S.Context, priority,
                            Attr.getAttributeSpellingListIndex()));
}

template <typename AttrTy>
static void handleAttrWithMessage(Sema &S, Decl *D,
                                  const AttributeList &Attr) {
  unsigned NumArgs = Attr.getNumArgs();
  if (NumArgs > 1) {
    S.Diag(Attr.getLoc(), diag::err_attribute_too_many_arguments) << 1;
    return;
  }

  // Handle the case where the attribute has a text message.
  StringRef Str;
  if (NumArgs == 1 && !S.checkStringLiteralArgumentAttr(Attr, 0, Str))
    return;

  D->addAttr(::new (S.Context) AttrTy(Attr.getRange(), S.Context, Str,
                                      Attr.getAttributeSpellingListIndex()));
}

static void handleObjCSuppresProtocolAttr(Sema &S, Decl *D,
                                          const AttributeList &Attr) {
  IdentifierLoc *Parm = Attr.isArgIdent(0) ? Attr.getArgAsIdent(0) : 0;

  if (!Parm) {
    S.Diag(D->getLocStart(), diag::err_objc_attr_not_id) << Attr.getName() << 1;
    return;
  }

  D->addAttr(::new (S.Context)
             ObjCSuppressProtocolAttr(Attr.getRange(), S.Context, Parm->Ident,
                                      Attr.getAttributeSpellingListIndex()));
}

static bool checkAvailabilityAttr(Sema &S, SourceRange Range,
                                  IdentifierInfo *Platform,
                                  VersionTuple Introduced,
                                  VersionTuple Deprecated,
                                  VersionTuple Obsoleted) {
  StringRef PlatformName
    = AvailabilityAttr::getPrettyPlatformName(Platform->getName());
  if (PlatformName.empty())
    PlatformName = Platform->getName();

  // Ensure that Introduced <= Deprecated <= Obsoleted (although not all
  // of these steps are needed).
  if (!Introduced.empty() && !Deprecated.empty() &&
      !(Introduced <= Deprecated)) {
    S.Diag(Range.getBegin(), diag::warn_availability_version_ordering)
      << 1 << PlatformName << Deprecated.getAsString()
      << 0 << Introduced.getAsString();
    return true;
  }

  if (!Introduced.empty() && !Obsoleted.empty() &&
      !(Introduced <= Obsoleted)) {
    S.Diag(Range.getBegin(), diag::warn_availability_version_ordering)
      << 2 << PlatformName << Obsoleted.getAsString()
      << 0 << Introduced.getAsString();
    return true;
  }

  if (!Deprecated.empty() && !Obsoleted.empty() &&
      !(Deprecated <= Obsoleted)) {
    S.Diag(Range.getBegin(), diag::warn_availability_version_ordering)
      << 2 << PlatformName << Obsoleted.getAsString()
      << 1 << Deprecated.getAsString();
    return true;
  }

  return false;
}

/// \brief Check whether the two versions match.
///
/// If either version tuple is empty, then they are assumed to match. If
/// \p BeforeIsOkay is true, then \p X can be less than or equal to \p Y.
static bool versionsMatch(const VersionTuple &X, const VersionTuple &Y,
                          bool BeforeIsOkay) {
  if (X.empty() || Y.empty())
    return true;

  if (X == Y)
    return true;

  if (BeforeIsOkay && X < Y)
    return true;

  return false;
}

AvailabilityAttr *Sema::mergeAvailabilityAttr(NamedDecl *D, SourceRange Range,
                                              IdentifierInfo *Platform,
                                              VersionTuple Introduced,
                                              VersionTuple Deprecated,
                                              VersionTuple Obsoleted,
                                              bool IsUnavailable,
                                              StringRef Message,
                                              bool Override,
                                              unsigned AttrSpellingListIndex) {
  VersionTuple MergedIntroduced = Introduced;
  VersionTuple MergedDeprecated = Deprecated;
  VersionTuple MergedObsoleted = Obsoleted;
  bool FoundAny = false;

  if (D->hasAttrs()) {
    AttrVec &Attrs = D->getAttrs();
    for (unsigned i = 0, e = Attrs.size(); i != e;) {
      const AvailabilityAttr *OldAA = dyn_cast<AvailabilityAttr>(Attrs[i]);
      if (!OldAA) {
        ++i;
        continue;
      }

      IdentifierInfo *OldPlatform = OldAA->getPlatform();
      if (OldPlatform != Platform) {
        ++i;
        continue;
      }

      FoundAny = true;
      VersionTuple OldIntroduced = OldAA->getIntroduced();
      VersionTuple OldDeprecated = OldAA->getDeprecated();
      VersionTuple OldObsoleted = OldAA->getObsoleted();
      bool OldIsUnavailable = OldAA->getUnavailable();

      if (!versionsMatch(OldIntroduced, Introduced, Override) ||
          !versionsMatch(Deprecated, OldDeprecated, Override) ||
          !versionsMatch(Obsoleted, OldObsoleted, Override) ||
          !(OldIsUnavailable == IsUnavailable ||
            (Override && !OldIsUnavailable && IsUnavailable))) {
        if (Override) {
          int Which = -1;
          VersionTuple FirstVersion;
          VersionTuple SecondVersion;
          if (!versionsMatch(OldIntroduced, Introduced, Override)) {
            Which = 0;
            FirstVersion = OldIntroduced;
            SecondVersion = Introduced;
          } else if (!versionsMatch(Deprecated, OldDeprecated, Override)) {
            Which = 1;
            FirstVersion = Deprecated;
            SecondVersion = OldDeprecated;
          } else if (!versionsMatch(Obsoleted, OldObsoleted, Override)) {
            Which = 2;
            FirstVersion = Obsoleted;
            SecondVersion = OldObsoleted;
          }

          if (Which == -1) {
            Diag(OldAA->getLocation(),
                 diag::warn_mismatched_availability_override_unavail)
              << AvailabilityAttr::getPrettyPlatformName(Platform->getName());
          } else {
            Diag(OldAA->getLocation(),
                 diag::warn_mismatched_availability_override)
              << Which
              << AvailabilityAttr::getPrettyPlatformName(Platform->getName())
              << FirstVersion.getAsString() << SecondVersion.getAsString();
          }
          Diag(Range.getBegin(), diag::note_overridden_method);
        } else {
          Diag(OldAA->getLocation(), diag::warn_mismatched_availability);
          Diag(Range.getBegin(), diag::note_previous_attribute);
        }

        Attrs.erase(Attrs.begin() + i);
        --e;
        continue;
      }

      VersionTuple MergedIntroduced2 = MergedIntroduced;
      VersionTuple MergedDeprecated2 = MergedDeprecated;
      VersionTuple MergedObsoleted2 = MergedObsoleted;

      if (MergedIntroduced2.empty())
        MergedIntroduced2 = OldIntroduced;
      if (MergedDeprecated2.empty())
        MergedDeprecated2 = OldDeprecated;
      if (MergedObsoleted2.empty())
        MergedObsoleted2 = OldObsoleted;

      if (checkAvailabilityAttr(*this, OldAA->getRange(), Platform,
                                MergedIntroduced2, MergedDeprecated2,
                                MergedObsoleted2)) {
        Attrs.erase(Attrs.begin() + i);
        --e;
        continue;
      }

      MergedIntroduced = MergedIntroduced2;
      MergedDeprecated = MergedDeprecated2;
      MergedObsoleted = MergedObsoleted2;
      ++i;
    }
  }

  if (FoundAny &&
      MergedIntroduced == Introduced &&
      MergedDeprecated == Deprecated &&
      MergedObsoleted == Obsoleted)
    return NULL;

  // Only create a new attribute if !Override, but we want to do
  // the checking.
  if (!checkAvailabilityAttr(*this, Range, Platform, MergedIntroduced,
                             MergedDeprecated, MergedObsoleted) &&
      !Override) {
    return ::new (Context) AvailabilityAttr(Range, Context, Platform,
                                            Introduced, Deprecated,
                                            Obsoleted, IsUnavailable, Message,
                                            AttrSpellingListIndex);
  }
  return NULL;
}

static void handleAvailabilityAttr(Sema &S, Decl *D,
                                   const AttributeList &Attr) {
  if (!checkAttributeNumArgs(S, Attr, 1))
    return;
  IdentifierLoc *Platform = Attr.getArgAsIdent(0);
  unsigned Index = Attr.getAttributeSpellingListIndex();
  
  IdentifierInfo *II = Platform->Ident;
  if (AvailabilityAttr::getPrettyPlatformName(II->getName()).empty())
    S.Diag(Platform->Loc, diag::warn_availability_unknown_platform)
      << Platform->Ident;

  NamedDecl *ND = dyn_cast<NamedDecl>(D);
  if (!ND) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_ignored) << Attr.getName();
    return;
  }

  AvailabilityChange Introduced = Attr.getAvailabilityIntroduced();
  AvailabilityChange Deprecated = Attr.getAvailabilityDeprecated();
  AvailabilityChange Obsoleted = Attr.getAvailabilityObsoleted();
  bool IsUnavailable = Attr.getUnavailableLoc().isValid();
  StringRef Str;
  if (const StringLiteral *SE =
          dyn_cast_or_null<StringLiteral>(Attr.getMessageExpr()))
    Str = SE->getString();

  AvailabilityAttr *NewAttr = S.mergeAvailabilityAttr(ND, Attr.getRange(), II,
                                                      Introduced.Version,
                                                      Deprecated.Version,
                                                      Obsoleted.Version,
                                                      IsUnavailable, Str,
                                                      /*Override=*/false,
                                                      Index);
  if (NewAttr)
    D->addAttr(NewAttr);
}

template <class T>
static T *mergeVisibilityAttr(Sema &S, Decl *D, SourceRange range,
                              typename T::VisibilityType value,
                              unsigned attrSpellingListIndex) {
  T *existingAttr = D->getAttr<T>();
  if (existingAttr) {
    typename T::VisibilityType existingValue = existingAttr->getVisibility();
    if (existingValue == value)
      return NULL;
    S.Diag(existingAttr->getLocation(), diag::err_mismatched_visibility);
    S.Diag(range.getBegin(), diag::note_previous_attribute);
    D->dropAttr<T>();
  }
  return ::new (S.Context) T(range, S.Context, value, attrSpellingListIndex);
}

VisibilityAttr *Sema::mergeVisibilityAttr(Decl *D, SourceRange Range,
                                          VisibilityAttr::VisibilityType Vis,
                                          unsigned AttrSpellingListIndex) {
  return ::mergeVisibilityAttr<VisibilityAttr>(*this, D, Range, Vis,
                                               AttrSpellingListIndex);
}

TypeVisibilityAttr *Sema::mergeTypeVisibilityAttr(Decl *D, SourceRange Range,
                                      TypeVisibilityAttr::VisibilityType Vis,
                                      unsigned AttrSpellingListIndex) {
  return ::mergeVisibilityAttr<TypeVisibilityAttr>(*this, D, Range, Vis,
                                                   AttrSpellingListIndex);
}

static void handleVisibilityAttr(Sema &S, Decl *D, const AttributeList &Attr,
                                 bool isTypeVisibility) {
  // Visibility attributes don't mean anything on a typedef.
  if (isa<TypedefNameDecl>(D)) {
    S.Diag(Attr.getRange().getBegin(), diag::warn_attribute_ignored)
      << Attr.getName();
    return;
  }

  // 'type_visibility' can only go on a type or namespace.
  if (isTypeVisibility &&
      !(isa<TagDecl>(D) ||
        isa<ObjCInterfaceDecl>(D) ||
        isa<NamespaceDecl>(D))) {
    S.Diag(Attr.getRange().getBegin(), diag::err_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedTypeOrNamespace;
    return;
  }

  // Check that the argument is a string literal.
  StringRef TypeStr;
  SourceLocation LiteralLoc;
  if (!S.checkStringLiteralArgumentAttr(Attr, 0, TypeStr, &LiteralLoc))
    return;

  VisibilityAttr::VisibilityType type;
  if (!VisibilityAttr::ConvertStrToVisibilityType(TypeStr, type)) {
    S.Diag(LiteralLoc, diag::warn_attribute_type_not_supported)
      << Attr.getName() << TypeStr;
    return;
  }
  
  // Complain about attempts to use protected visibility on targets
  // (like Darwin) that don't support it.
  if (type == VisibilityAttr::Protected &&
      !S.Context.getTargetInfo().hasProtectedVisibility()) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_protected_visibility);
    type = VisibilityAttr::Default;
  }

  unsigned Index = Attr.getAttributeSpellingListIndex();
  clang::Attr *newAttr;
  if (isTypeVisibility) {
    newAttr = S.mergeTypeVisibilityAttr(D, Attr.getRange(),
                                    (TypeVisibilityAttr::VisibilityType) type,
                                        Index);
  } else {
    newAttr = S.mergeVisibilityAttr(D, Attr.getRange(), type, Index);
  }
  if (newAttr)
    D->addAttr(newAttr);
}

static void handleObjCMethodFamilyAttr(Sema &S, Decl *decl,
                                       const AttributeList &Attr) {
  ObjCMethodDecl *method = cast<ObjCMethodDecl>(decl);
  if (!Attr.isArgIdent(0)) {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_n_type)
      << Attr.getName() << 1 << AANT_ArgumentIdentifier;
    return;
  }

  IdentifierLoc *IL = Attr.getArgAsIdent(0);
  ObjCMethodFamilyAttr::FamilyKind F;
  if (!ObjCMethodFamilyAttr::ConvertStrToFamilyKind(IL->Ident->getName(), F)) {
    S.Diag(IL->Loc, diag::warn_attribute_type_not_supported) << Attr.getName()
      << IL->Ident;
    return;
  }

  if (F == ObjCMethodFamilyAttr::OMF_init && 
      !method->getResultType()->isObjCObjectPointerType()) {
    S.Diag(method->getLocation(), diag::err_init_method_bad_return_type)
      << method->getResultType();
    // Ignore the attribute.
    return;
  }

  method->addAttr(new (S.Context) ObjCMethodFamilyAttr(Attr.getRange(),
                                                       S.Context, F));
}

static void handleObjCNSObject(Sema &S, Decl *D, const AttributeList &Attr) {
  if (TypedefNameDecl *TD = dyn_cast<TypedefNameDecl>(D)) {
    QualType T = TD->getUnderlyingType();
    if (!T->isCARCBridgableType()) {
      S.Diag(TD->getLocation(), diag::err_nsobject_attribute);
      return;
    }
  }
  else if (ObjCPropertyDecl *PD = dyn_cast<ObjCPropertyDecl>(D)) {
    QualType T = PD->getType();
    if (!T->isCARCBridgableType()) {
      S.Diag(PD->getLocation(), diag::err_nsobject_attribute);
      return;
    }
  }
  else {
    // It is okay to include this attribute on properties, e.g.:
    //
    //  @property (retain, nonatomic) struct Bork *Q __attribute__((NSObject));
    //
    // In this case it follows tradition and suppresses an error in the above
    // case.    
    S.Diag(D->getLocation(), diag::warn_nsobject_attribute);
  }
  D->addAttr(::new (S.Context)
             ObjCNSObjectAttr(Attr.getRange(), S.Context,
                              Attr.getAttributeSpellingListIndex()));
}

static void handleBlocksAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (!Attr.isArgIdent(0)) {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_n_type)
      << Attr.getName() << 1 << AANT_ArgumentIdentifier;
    return;
  }

  IdentifierInfo *II = Attr.getArgAsIdent(0)->Ident;
  BlocksAttr::BlockType type;
  if (!BlocksAttr::ConvertStrToBlockType(II->getName(), type)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_type_not_supported)
      << Attr.getName() << II;
    return;
  }

  D->addAttr(::new (S.Context)
             BlocksAttr(Attr.getRange(), S.Context, type,
                        Attr.getAttributeSpellingListIndex()));
}

static void handleSentinelAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  // check the attribute arguments.
  if (Attr.getNumArgs() > 2) {
    S.Diag(Attr.getLoc(), diag::err_attribute_too_many_arguments) << 2;
    return;
  }

  unsigned sentinel = (unsigned)SentinelAttr::DefaultSentinel;
  if (Attr.getNumArgs() > 0) {
    Expr *E = Attr.getArgAsExpr(0);
    llvm::APSInt Idx(32);
    if (E->isTypeDependent() || E->isValueDependent() ||
        !E->isIntegerConstantExpr(Idx, S.Context)) {
      S.Diag(Attr.getLoc(), diag::err_attribute_argument_n_type)
        << Attr.getName() << 1 << AANT_ArgumentIntegerConstant
        << E->getSourceRange();
      return;
    }

    if (Idx.isSigned() && Idx.isNegative()) {
      S.Diag(Attr.getLoc(), diag::err_attribute_sentinel_less_than_zero)
        << E->getSourceRange();
      return;
    }

    sentinel = Idx.getZExtValue();
  }

  unsigned nullPos = (unsigned)SentinelAttr::DefaultNullPos;
  if (Attr.getNumArgs() > 1) {
    Expr *E = Attr.getArgAsExpr(1);
    llvm::APSInt Idx(32);
    if (E->isTypeDependent() || E->isValueDependent() ||
        !E->isIntegerConstantExpr(Idx, S.Context)) {
      S.Diag(Attr.getLoc(), diag::err_attribute_argument_n_type)
        << Attr.getName() << 2 << AANT_ArgumentIntegerConstant
        << E->getSourceRange();
      return;
    }
    nullPos = Idx.getZExtValue();

    if ((Idx.isSigned() && Idx.isNegative()) || nullPos > 1) {
      // FIXME: This error message could be improved, it would be nice
      // to say what the bounds actually are.
      S.Diag(Attr.getLoc(), diag::err_attribute_sentinel_not_zero_or_one)
        << E->getSourceRange();
      return;
    }
  }

  if (FunctionDecl *FD = dyn_cast<FunctionDecl>(D)) {
    const FunctionType *FT = FD->getType()->castAs<FunctionType>();
    if (isa<FunctionNoProtoType>(FT)) {
      S.Diag(Attr.getLoc(), diag::warn_attribute_sentinel_named_arguments);
      return;
    }

    if (!cast<FunctionProtoType>(FT)->isVariadic()) {
      S.Diag(Attr.getLoc(), diag::warn_attribute_sentinel_not_variadic) << 0;
      return;
    }
  } else if (ObjCMethodDecl *MD = dyn_cast<ObjCMethodDecl>(D)) {
    if (!MD->isVariadic()) {
      S.Diag(Attr.getLoc(), diag::warn_attribute_sentinel_not_variadic) << 0;
      return;
    }
  } else if (BlockDecl *BD = dyn_cast<BlockDecl>(D)) {
    if (!BD->isVariadic()) {
      S.Diag(Attr.getLoc(), diag::warn_attribute_sentinel_not_variadic) << 1;
      return;
    }
  } else if (const VarDecl *V = dyn_cast<VarDecl>(D)) {
    QualType Ty = V->getType();
    if (Ty->isBlockPointerType() || Ty->isFunctionPointerType()) {
      const FunctionType *FT = Ty->isFunctionPointerType() ? getFunctionType(D)
       : Ty->getAs<BlockPointerType>()->getPointeeType()->getAs<FunctionType>();
      if (!cast<FunctionProtoType>(FT)->isVariadic()) {
        int m = Ty->isFunctionPointerType() ? 0 : 1;
        S.Diag(Attr.getLoc(), diag::warn_attribute_sentinel_not_variadic) << m;
        return;
      }
    } else {
      S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
        << Attr.getName() << ExpectedFunctionMethodOrBlock;
      return;
    }
  } else {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedFunctionMethodOrBlock;
    return;
  }
  D->addAttr(::new (S.Context)
             SentinelAttr(Attr.getRange(), S.Context, sentinel, nullPos,
                          Attr.getAttributeSpellingListIndex()));
}

static void handleWarnUnusedResult(Sema &S, Decl *D, const AttributeList &Attr) {
  if (!isFunction(D) && !isa<ObjCMethodDecl>(D) && !isa<CXXRecordDecl>(D)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedFunctionMethodOrClass;
    return;
  }

  if (isFunction(D) && getFunctionType(D)->getResultType()->isVoidType()) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_void_function_method)
      << Attr.getName() << 0;
    return;
  }
  if (const ObjCMethodDecl *MD = dyn_cast<ObjCMethodDecl>(D))
    if (MD->getResultType()->isVoidType()) {
      S.Diag(Attr.getLoc(), diag::warn_attribute_void_function_method)
      << Attr.getName() << 1;
      return;
    }
  
  D->addAttr(::new (S.Context) 
             WarnUnusedResultAttr(Attr.getRange(), S.Context,
                                  Attr.getAttributeSpellingListIndex()));
}

static void handleWeakImportAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  // weak_import only applies to variable & function declarations.
  bool isDef = false;
  if (!D->canBeWeakImported(isDef)) {
    if (isDef)
      S.Diag(Attr.getLoc(), diag::warn_attribute_invalid_on_definition)
        << "weak_import";
    else if (isa<ObjCPropertyDecl>(D) || isa<ObjCMethodDecl>(D) ||
             (S.Context.getTargetInfo().getTriple().isOSDarwin() &&
              (isa<ObjCInterfaceDecl>(D) || isa<EnumDecl>(D)))) {
      // Nothing to warn about here.
    } else
      S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
        << Attr.getName() << ExpectedVariableOrFunction;

    return;
  }

  D->addAttr(::new (S.Context)
             WeakImportAttr(Attr.getRange(), S.Context,
                            Attr.getAttributeSpellingListIndex()));
}

// Handles reqd_work_group_size and work_group_size_hint.
template <typename WorkGroupAttr>
static void handleWorkGroupSize(Sema &S, Decl *D,
                                const AttributeList &Attr) {
  uint32_t WGSize[3];
  for (unsigned i = 0; i < 3; ++i)
    if (!checkUInt32Argument(S, Attr, Attr.getArgAsExpr(i), WGSize[i], i))
      return;

  WorkGroupAttr *Existing = D->getAttr<WorkGroupAttr>();
  if (Existing && !(Existing->getXDim() == WGSize[0] &&
                    Existing->getYDim() == WGSize[1] &&
                    Existing->getZDim() == WGSize[2]))
    S.Diag(Attr.getLoc(), diag::warn_duplicate_attribute) << Attr.getName();

  D->addAttr(::new (S.Context) WorkGroupAttr(Attr.getRange(), S.Context,
                                             WGSize[0], WGSize[1], WGSize[2],
                                       Attr.getAttributeSpellingListIndex()));
}

static void handleVecTypeHint(Sema &S, Decl *D, const AttributeList &Attr) {
  if (!Attr.hasParsedType()) {
    S.Diag(Attr.getLoc(), diag::err_attribute_wrong_number_arguments)
      << Attr.getName() << 1;
    return;
  }

  TypeSourceInfo *ParmTSI = 0;
  QualType ParmType = S.GetTypeFromParser(Attr.getTypeArg(), &ParmTSI);
  assert(ParmTSI && "no type source info for attribute argument");

  if (!ParmType->isExtVectorType() && !ParmType->isFloatingType() &&
      (ParmType->isBooleanType() ||
       !ParmType->isIntegralType(S.getASTContext()))) {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_vec_type_hint)
        << ParmType;
    return;
  }

  if (VecTypeHintAttr *A = D->getAttr<VecTypeHintAttr>()) {
    if (!S.Context.hasSameType(A->getTypeHint(), ParmType)) {
      S.Diag(Attr.getLoc(), diag::warn_duplicate_attribute) << Attr.getName();
      return;
    }
  }

  D->addAttr(::new (S.Context) VecTypeHintAttr(Attr.getLoc(), S.Context,
                                               ParmTSI));
}

SectionAttr *Sema::mergeSectionAttr(Decl *D, SourceRange Range,
                                    StringRef Name,
                                    unsigned AttrSpellingListIndex) {
  if (SectionAttr *ExistingAttr = D->getAttr<SectionAttr>()) {
    if (ExistingAttr->getName() == Name)
      return NULL;
    Diag(ExistingAttr->getLocation(), diag::warn_mismatched_section);
    Diag(Range.getBegin(), diag::note_previous_attribute);
    return NULL;
  }
  return ::new (Context) SectionAttr(Range, Context, Name,
                                     AttrSpellingListIndex);
}

static void handleSectionAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  // Make sure that there is a string literal as the sections's single
  // argument.
  StringRef Str;
  SourceLocation LiteralLoc;
  if (!S.checkStringLiteralArgumentAttr(Attr, 0, Str, &LiteralLoc))
    return;

  // If the target wants to validate the section specifier, make it happen.
  std::string Error = S.Context.getTargetInfo().isValidSectionSpecifier(Str);
  if (!Error.empty()) {
    S.Diag(LiteralLoc, diag::err_attribute_section_invalid_for_target)
    << Error;
    return;
  }

  // This attribute cannot be applied to local variables.
  if (isa<VarDecl>(D) && cast<VarDecl>(D)->hasLocalStorage()) {
    S.Diag(LiteralLoc, diag::err_attribute_section_local_variable);
    return;
  }
  
  unsigned Index = Attr.getAttributeSpellingListIndex();
  SectionAttr *NewAttr = S.mergeSectionAttr(D, Attr.getRange(), Str, Index);
  if (NewAttr)
    D->addAttr(NewAttr);
}


static void handleNothrowAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (NoThrowAttr *Existing = D->getAttr<NoThrowAttr>()) {
    if (Existing->getLocation().isInvalid())
      Existing->setRange(Attr.getRange());
  } else {
    D->addAttr(::new (S.Context)
               NoThrowAttr(Attr.getRange(), S.Context,
                           Attr.getAttributeSpellingListIndex()));
  }
}

static void handleConstAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (ConstAttr *Existing = D->getAttr<ConstAttr>()) {
   if (Existing->getLocation().isInvalid())
     Existing->setRange(Attr.getRange());
  } else {
    D->addAttr(::new (S.Context)
               ConstAttr(Attr.getRange(), S.Context,
                         Attr.getAttributeSpellingListIndex() ));
  }
}

static void handleCleanupAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  VarDecl *VD = cast<VarDecl>(D);
  if (!VD->hasLocalStorage()) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_ignored) << Attr.getName();
    return;
  }

  Expr *E = Attr.getArgAsExpr(0);
  SourceLocation Loc = E->getExprLoc();
  FunctionDecl *FD = 0;
  DeclarationNameInfo NI;

  // gcc only allows for simple identifiers. Since we support more than gcc, we
  // will warn the user.
  if (DeclRefExpr *DRE = dyn_cast<DeclRefExpr>(E)) {
    if (DRE->hasQualifier())
      S.Diag(Loc, diag::warn_cleanup_ext);
    FD = dyn_cast<FunctionDecl>(DRE->getDecl());
    NI = DRE->getNameInfo();
    if (!FD) {
      S.Diag(Loc, diag::err_attribute_cleanup_arg_not_function) << 1
        << NI.getName();
      return;
    }
  } else if (UnresolvedLookupExpr *ULE = dyn_cast<UnresolvedLookupExpr>(E)) {
    if (ULE->hasExplicitTemplateArgs())
      S.Diag(Loc, diag::warn_cleanup_ext);
    FD = S.ResolveSingleFunctionTemplateSpecialization(ULE, true);
    NI = ULE->getNameInfo();
    if (!FD) {
      S.Diag(Loc, diag::err_attribute_cleanup_arg_not_function) << 2
        << NI.getName();
      if (ULE->getType() == S.Context.OverloadTy)
        S.NoteAllOverloadCandidates(ULE);
      return;
    }
  } else {
    S.Diag(Loc, diag::err_attribute_cleanup_arg_not_function) << 0;
    return;
  }

  if (FD->getNumParams() != 1) {
    S.Diag(Loc, diag::err_attribute_cleanup_func_must_take_one_arg)
      << NI.getName();
    return;
  }

  // We're currently more strict than GCC about what function types we accept.
  // If this ever proves to be a problem it should be easy to fix.
  QualType Ty = S.Context.getPointerType(VD->getType());
  QualType ParamTy = FD->getParamDecl(0)->getType();
  if (S.CheckAssignmentConstraints(FD->getParamDecl(0)->getLocation(),
                                   ParamTy, Ty) != Sema::Compatible) {
    S.Diag(Loc, diag::err_attribute_cleanup_func_arg_incompatible_type)
      << NI.getName() << ParamTy << Ty;
    return;
  }

  D->addAttr(::new (S.Context)
             CleanupAttr(Attr.getRange(), S.Context, FD,
                         Attr.getAttributeSpellingListIndex()));
}

/// Handle __attribute__((format_arg((idx)))) attribute based on
/// http://gcc.gnu.org/onlinedocs/gcc/Function-Attributes.html
static void handleFormatArgAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (!isFunctionOrMethod(D) || !hasFunctionProto(D)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedFunction;
    return;
  }

  Expr *IdxExpr = Attr.getArgAsExpr(0);
  uint64_t ArgIdx;
  if (!checkFunctionOrMethodArgumentIndex(S, D, Attr.getName()->getName(),
                                          Attr.getLoc(), 1, IdxExpr, ArgIdx))
    return;

  // make sure the format string is really a string
  QualType Ty = getFunctionOrMethodArgType(D, ArgIdx);

  bool not_nsstring_type = !isNSStringType(Ty, S.Context);
  if (not_nsstring_type &&
      !isCFStringType(Ty, S.Context) &&
      (!Ty->isPointerType() ||
       !Ty->getAs<PointerType>()->getPointeeType()->isCharType())) {
    // FIXME: Should highlight the actual expression that has the wrong type.
    S.Diag(Attr.getLoc(), diag::err_format_attribute_not)
    << (not_nsstring_type ? "a string type" : "an NSString")
       << IdxExpr->getSourceRange();
    return;
  }
  Ty = getFunctionOrMethodResultType(D);
  if (!isNSStringType(Ty, S.Context) &&
      !isCFStringType(Ty, S.Context) &&
      (!Ty->isPointerType() ||
       !Ty->getAs<PointerType>()->getPointeeType()->isCharType())) {
    // FIXME: Should highlight the actual expression that has the wrong type.
    S.Diag(Attr.getLoc(), diag::err_format_attribute_result_not)
    << (not_nsstring_type ? "string type" : "NSString")
       << IdxExpr->getSourceRange();
    return;
  }

  // We cannot use the ArgIdx returned from checkFunctionOrMethodArgumentIndex
  // because that has corrected for the implicit this parameter, and is zero-
  // based.  The attribute expects what the user wrote explicitly.
  llvm::APSInt Val;
  IdxExpr->EvaluateAsInt(Val, S.Context);

  D->addAttr(::new (S.Context)
             FormatArgAttr(Attr.getRange(), S.Context, Val.getZExtValue(),
                           Attr.getAttributeSpellingListIndex()));
}

enum FormatAttrKind {
  CFStringFormat,
  NSStringFormat,
  StrftimeFormat,
  SupportedFormat,
  IgnoredFormat,
  InvalidFormat
};

/// getFormatAttrKind - Map from format attribute names to supported format
/// types.
static FormatAttrKind getFormatAttrKind(StringRef Format) {
  return llvm::StringSwitch<FormatAttrKind>(Format)
    // Check for formats that get handled specially.
    .Case("NSString", NSStringFormat)
    .Case("CFString", CFStringFormat)
    .Case("strftime", StrftimeFormat)

    // Otherwise, check for supported formats.
    .Cases("scanf", "printf", "printf0", "strfmon", SupportedFormat)
    .Cases("cmn_err", "vcmn_err", "zcmn_err", SupportedFormat)
    .Case("kprintf", SupportedFormat) // OpenBSD.

    .Cases("gcc_diag", "gcc_cdiag", "gcc_cxxdiag", "gcc_tdiag", IgnoredFormat)
    .Default(InvalidFormat);
}

/// Handle __attribute__((init_priority(priority))) attributes based on
/// http://gcc.gnu.org/onlinedocs/gcc/C_002b_002b-Attributes.html
static void handleInitPriorityAttr(Sema &S, Decl *D,
                                   const AttributeList &Attr) {
  if (!S.getLangOpts().CPlusPlus) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_ignored) << Attr.getName();
    return;
  }
  
  if (S.getCurFunctionOrMethodDecl()) {
    S.Diag(Attr.getLoc(), diag::err_init_priority_object_attr);
    Attr.setInvalid();
    return;
  }
  QualType T = cast<VarDecl>(D)->getType();
  if (S.Context.getAsArrayType(T))
    T = S.Context.getBaseElementType(T);
  if (!T->getAs<RecordType>()) {
    S.Diag(Attr.getLoc(), diag::err_init_priority_object_attr);
    Attr.setInvalid();
    return;
  }

  Expr *E = Attr.getArgAsExpr(0);
  uint32_t prioritynum;
  if (!checkUInt32Argument(S, Attr, E, prioritynum)) {
    Attr.setInvalid();
    return;
  }

  if (prioritynum < 101 || prioritynum > 65535) {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_outof_range)
      << E->getSourceRange();
    Attr.setInvalid();
    return;
  }
  D->addAttr(::new (S.Context)
             InitPriorityAttr(Attr.getRange(), S.Context, prioritynum,
                              Attr.getAttributeSpellingListIndex()));
}

FormatAttr *Sema::mergeFormatAttr(Decl *D, SourceRange Range,
                                  IdentifierInfo *Format, int FormatIdx,
                                  int FirstArg,
                                  unsigned AttrSpellingListIndex) {
  // Check whether we already have an equivalent format attribute.
  for (specific_attr_iterator<FormatAttr>
         i = D->specific_attr_begin<FormatAttr>(),
         e = D->specific_attr_end<FormatAttr>();
       i != e ; ++i) {
    FormatAttr *f = *i;
    if (f->getType() == Format &&
        f->getFormatIdx() == FormatIdx &&
        f->getFirstArg() == FirstArg) {
      // If we don't have a valid location for this attribute, adopt the
      // location.
      if (f->getLocation().isInvalid())
        f->setRange(Range);
      return NULL;
    }
  }

  return ::new (Context) FormatAttr(Range, Context, Format, FormatIdx,
                                    FirstArg, AttrSpellingListIndex);
}

/// Handle __attribute__((format(type,idx,firstarg))) attributes based on
/// http://gcc.gnu.org/onlinedocs/gcc/Function-Attributes.html
static void handleFormatAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (!Attr.isArgIdent(0)) {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_n_type)
      << Attr.getName() << 1 << AANT_ArgumentIdentifier;
    return;
  }

  if (!isFunctionOrMethodOrBlock(D) || !hasFunctionProto(D)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedFunction;
    return;
  }

  // In C++ the implicit 'this' function parameter also counts, and they are
  // counted from one.
  bool HasImplicitThisParam = isInstanceMethod(D);
  unsigned NumArgs  = getFunctionOrMethodNumArgs(D) + HasImplicitThisParam;

  IdentifierInfo *II = Attr.getArgAsIdent(0)->Ident;
  StringRef Format = II->getName();

  // Normalize the argument, __foo__ becomes foo.
  if (Format.startswith("__") && Format.endswith("__")) {
    Format = Format.substr(2, Format.size() - 4);
    // If we've modified the string name, we need a new identifier for it.
    II = &S.Context.Idents.get(Format);
  }

  // Check for supported formats.
  FormatAttrKind Kind = getFormatAttrKind(Format);
  
  if (Kind == IgnoredFormat)
    return;
  
  if (Kind == InvalidFormat) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_type_not_supported)
      << "format" << II->getName();
    return;
  }

  // checks for the 2nd argument
  Expr *IdxExpr = Attr.getArgAsExpr(1);
  uint32_t Idx;
  if (!checkUInt32Argument(S, Attr, IdxExpr, Idx, 2))
    return;

  if (Idx < 1 || Idx > NumArgs) {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_out_of_bounds)
      << "format" << 2 << IdxExpr->getSourceRange();
    return;
  }

  // FIXME: Do we need to bounds check?
  unsigned ArgIdx = Idx - 1;

  if (HasImplicitThisParam) {
    if (ArgIdx == 0) {
      S.Diag(Attr.getLoc(),
             diag::err_format_attribute_implicit_this_format_string)
        << IdxExpr->getSourceRange();
      return;
    }
    ArgIdx--;
  }

  // make sure the format string is really a string
  QualType Ty = getFunctionOrMethodArgType(D, ArgIdx);

  if (Kind == CFStringFormat) {
    if (!isCFStringType(Ty, S.Context)) {
      S.Diag(Attr.getLoc(), diag::err_format_attribute_not)
        << "a CFString" << IdxExpr->getSourceRange();
      return;
    }
  } else if (Kind == NSStringFormat) {
    // FIXME: do we need to check if the type is NSString*?  What are the
    // semantics?
    if (!isNSStringType(Ty, S.Context)) {
      // FIXME: Should highlight the actual expression that has the wrong type.
      S.Diag(Attr.getLoc(), diag::err_format_attribute_not)
        << "an NSString" << IdxExpr->getSourceRange();
      return;
    }
  } else if (!Ty->isPointerType() ||
             !Ty->getAs<PointerType>()->getPointeeType()->isCharType()) {
    // FIXME: Should highlight the actual expression that has the wrong type.
    S.Diag(Attr.getLoc(), diag::err_format_attribute_not)
      << "a string type" << IdxExpr->getSourceRange();
    return;
  }

  // check the 3rd argument
  Expr *FirstArgExpr = Attr.getArgAsExpr(2);
  uint32_t FirstArg;
  if (!checkUInt32Argument(S, Attr, FirstArgExpr, FirstArg, 3))
    return;

  // check if the function is variadic if the 3rd argument non-zero
  if (FirstArg != 0) {
    if (isFunctionOrMethodVariadic(D)) {
      ++NumArgs; // +1 for ...
    } else {
      S.Diag(D->getLocation(), diag::err_format_attribute_requires_variadic);
      return;
    }
  }

  // strftime requires FirstArg to be 0 because it doesn't read from any
  // variable the input is just the current time + the format string.
  if (Kind == StrftimeFormat) {
    if (FirstArg != 0) {
      S.Diag(Attr.getLoc(), diag::err_format_strftime_third_parameter)
        << FirstArgExpr->getSourceRange();
      return;
    }
  // if 0 it disables parameter checking (to use with e.g. va_list)
  } else if (FirstArg != 0 && FirstArg != NumArgs) {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_out_of_bounds)
      << "format" << 3 << FirstArgExpr->getSourceRange();
    return;
  }

  FormatAttr *NewAttr = S.mergeFormatAttr(D, Attr.getRange(), II,
                                          Idx, FirstArg,
                                          Attr.getAttributeSpellingListIndex());
  if (NewAttr)
    D->addAttr(NewAttr);
}

static void handleTransparentUnionAttr(Sema &S, Decl *D,
                                       const AttributeList &Attr) {
  // Try to find the underlying union declaration.
  RecordDecl *RD = 0;
  TypedefNameDecl *TD = dyn_cast<TypedefNameDecl>(D);
  if (TD && TD->getUnderlyingType()->isUnionType())
    RD = TD->getUnderlyingType()->getAsUnionType()->getDecl();
  else
    RD = dyn_cast<RecordDecl>(D);

  if (!RD || !RD->isUnion()) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedUnion;
    return;
  }

  if (!RD->isCompleteDefinition()) {
    S.Diag(Attr.getLoc(),
        diag::warn_transparent_union_attribute_not_definition);
    return;
  }

  RecordDecl::field_iterator Field = RD->field_begin(),
                          FieldEnd = RD->field_end();
  if (Field == FieldEnd) {
    S.Diag(Attr.getLoc(), diag::warn_transparent_union_attribute_zero_fields);
    return;
  }

  FieldDecl *FirstField = *Field;
  QualType FirstType = FirstField->getType();
  if (FirstType->hasFloatingRepresentation() || FirstType->isVectorType()) {
    S.Diag(FirstField->getLocation(),
           diag::warn_transparent_union_attribute_floating)
      << FirstType->isVectorType() << FirstType;
    return;
  }

  uint64_t FirstSize = S.Context.getTypeSize(FirstType);
  uint64_t FirstAlign = S.Context.getTypeAlign(FirstType);
  for (; Field != FieldEnd; ++Field) {
    QualType FieldType = Field->getType();
    if (S.Context.getTypeSize(FieldType) != FirstSize ||
        S.Context.getTypeAlign(FieldType) != FirstAlign) {
      // Warn if we drop the attribute.
      bool isSize = S.Context.getTypeSize(FieldType) != FirstSize;
      unsigned FieldBits = isSize? S.Context.getTypeSize(FieldType)
                                 : S.Context.getTypeAlign(FieldType);
      S.Diag(Field->getLocation(),
          diag::warn_transparent_union_attribute_field_size_align)
        << isSize << Field->getDeclName() << FieldBits;
      unsigned FirstBits = isSize? FirstSize : FirstAlign;
      S.Diag(FirstField->getLocation(),
             diag::note_transparent_union_first_field_size_align)
        << isSize << FirstBits;
      return;
    }
  }

  RD->addAttr(::new (S.Context)
              TransparentUnionAttr(Attr.getRange(), S.Context,
                                   Attr.getAttributeSpellingListIndex()));
}

static void handleAnnotateAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  // Make sure that there is a string literal as the annotation's single
  // argument.
  StringRef Str;
  if (!S.checkStringLiteralArgumentAttr(Attr, 0, Str))
    return;

  // Don't duplicate annotations that are already set.
  for (specific_attr_iterator<AnnotateAttr>
       i = D->specific_attr_begin<AnnotateAttr>(),
       e = D->specific_attr_end<AnnotateAttr>(); i != e; ++i) {
    if ((*i)->getAnnotation() == Str)
      return;
  }
  
  D->addAttr(::new (S.Context)
             AnnotateAttr(Attr.getRange(), S.Context, Str,
                          Attr.getAttributeSpellingListIndex()));
}

static void handleAlignedAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  // check the attribute arguments.
  if (Attr.getNumArgs() > 1) {
    S.Diag(Attr.getLoc(), diag::err_attribute_wrong_number_arguments)
      << Attr.getName() << 1;
    return;
  }

  if (Attr.getNumArgs() == 0) {
    D->addAttr(::new (S.Context) AlignedAttr(Attr.getRange(), S.Context,
               true, 0, Attr.getAttributeSpellingListIndex()));
    return;
  }

  Expr *E = Attr.getArgAsExpr(0);
  if (Attr.isPackExpansion() && !E->containsUnexpandedParameterPack()) {
    S.Diag(Attr.getEllipsisLoc(),
           diag::err_pack_expansion_without_parameter_packs);
    return;
  }

  if (!Attr.isPackExpansion() && S.DiagnoseUnexpandedParameterPack(E))
    return;

  S.AddAlignedAttr(Attr.getRange(), D, E, Attr.getAttributeSpellingListIndex(),
                   Attr.isPackExpansion());
}

void Sema::AddAlignedAttr(SourceRange AttrRange, Decl *D, Expr *E,
                          unsigned SpellingListIndex, bool IsPackExpansion) {
  AlignedAttr TmpAttr(AttrRange, Context, true, E, SpellingListIndex);
  SourceLocation AttrLoc = AttrRange.getBegin();

  // C++11 alignas(...) and C11 _Alignas(...) have additional requirements.
  if (TmpAttr.isAlignas()) {
    // C++11 [dcl.align]p1:
    //   An alignment-specifier may be applied to a variable or to a class
    //   data member, but it shall not be applied to a bit-field, a function
    //   parameter, the formal parameter of a catch clause, or a variable
    //   declared with the register storage class specifier. An
    //   alignment-specifier may also be applied to the declaration of a class
    //   or enumeration type.
    // C11 6.7.5/2:
    //   An alignment attribute shall not be specified in a declaration of
    //   a typedef, or a bit-field, or a function, or a parameter, or an
    //   object declared with the register storage-class specifier.
    int DiagKind = -1;
    if (isa<ParmVarDecl>(D)) {
      DiagKind = 0;
    } else if (VarDecl *VD = dyn_cast<VarDecl>(D)) {
      if (VD->getStorageClass() == SC_Register)
        DiagKind = 1;
      if (VD->isExceptionVariable())
        DiagKind = 2;
    } else if (FieldDecl *FD = dyn_cast<FieldDecl>(D)) {
      if (FD->isBitField())
        DiagKind = 3;
    } else if (!isa<TagDecl>(D)) {
      Diag(AttrLoc, diag::err_attribute_wrong_decl_type)
        << (TmpAttr.isC11() ? "'_Alignas'" : "'alignas'")
        << (TmpAttr.isC11() ? ExpectedVariableOrField
                            : ExpectedVariableFieldOrTag);
      return;
    }
    if (DiagKind != -1) {
      Diag(AttrLoc, diag::err_alignas_attribute_wrong_decl_type)
        << TmpAttr.isC11() << DiagKind;
      return;
    }
  }

  if (E->isTypeDependent() || E->isValueDependent()) {
    // Save dependent expressions in the AST to be instantiated.
    AlignedAttr *AA = ::new (Context) AlignedAttr(TmpAttr);
    AA->setPackExpansion(IsPackExpansion);
    D->addAttr(AA);
    return;
  }

  // FIXME: Cache the number on the Attr object?
  llvm::APSInt Alignment(32);
  ExprResult ICE
    = VerifyIntegerConstantExpression(E, &Alignment,
        diag::err_aligned_attribute_argument_not_int,
        /*AllowFold*/ false);
  if (ICE.isInvalid())
    return;

  // C++11 [dcl.align]p2:
  //   -- if the constant expression evaluates to zero, the alignment
  //      specifier shall have no effect
  // C11 6.7.5p6:
  //   An alignment specification of zero has no effect.
  if (!(TmpAttr.isAlignas() && !Alignment) &&
      !llvm::isPowerOf2_64(Alignment.getZExtValue())) {
    Diag(AttrLoc, diag::err_attribute_aligned_not_power_of_two)
      << E->getSourceRange();
    return;
  }

  if (TmpAttr.isDeclspec()) {
    // We've already verified it's a power of 2, now let's make sure it's
    // 8192 or less.
    if (Alignment.getZExtValue() > 8192) {
      Diag(AttrLoc, diag::err_attribute_aligned_greater_than_8192)
        << E->getSourceRange();
      return;
    }
  }

  AlignedAttr *AA = ::new (Context) AlignedAttr(AttrRange, Context, true,
                                                ICE.take(), SpellingListIndex);
  AA->setPackExpansion(IsPackExpansion);
  D->addAttr(AA);
}

void Sema::AddAlignedAttr(SourceRange AttrRange, Decl *D, TypeSourceInfo *TS,
                          unsigned SpellingListIndex, bool IsPackExpansion) {
  // FIXME: Cache the number on the Attr object if non-dependent?
  // FIXME: Perform checking of type validity
  AlignedAttr *AA = ::new (Context) AlignedAttr(AttrRange, Context, false, TS,
                                                SpellingListIndex);
  AA->setPackExpansion(IsPackExpansion);
  D->addAttr(AA);
}

void Sema::CheckAlignasUnderalignment(Decl *D) {
  assert(D->hasAttrs() && "no attributes on decl");

  QualType Ty;
  if (ValueDecl *VD = dyn_cast<ValueDecl>(D))
    Ty = VD->getType();
  else
    Ty = Context.getTagDeclType(cast<TagDecl>(D));
  if (Ty->isDependentType() || Ty->isIncompleteType())
    return;

  // C++11 [dcl.align]p5, C11 6.7.5/4:
  //   The combined effect of all alignment attributes in a declaration shall
  //   not specify an alignment that is less strict than the alignment that
  //   would otherwise be required for the entity being declared.
  AlignedAttr *AlignasAttr = 0;
  unsigned Align = 0;
  for (specific_attr_iterator<AlignedAttr>
         I = D->specific_attr_begin<AlignedAttr>(),
         E = D->specific_attr_end<AlignedAttr>(); I != E; ++I) {
    if (I->isAlignmentDependent())
      return;
    if (I->isAlignas())
      AlignasAttr = *I;
    Align = std::max(Align, I->getAlignment(Context));
  }

  if (AlignasAttr && Align) {
    CharUnits RequestedAlign = Context.toCharUnitsFromBits(Align);
    CharUnits NaturalAlign = Context.getTypeAlignInChars(Ty);
    if (NaturalAlign > RequestedAlign)
      Diag(AlignasAttr->getLocation(), diag::err_alignas_underaligned)
        << Ty << (unsigned)NaturalAlign.getQuantity();
  }
}

/// handleModeAttr - This attribute modifies the width of a decl with primitive
/// type.
///
/// Despite what would be logical, the mode attribute is a decl attribute, not a
/// type attribute: 'int ** __attribute((mode(HI))) *G;' tries to make 'G' be
/// HImode, not an intermediate pointer.
static void handleModeAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  // This attribute isn't documented, but glibc uses it.  It changes
  // the width of an int or unsigned int to the specified size.
  if (!Attr.isArgIdent(0)) {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_type) << Attr.getName()
      << AANT_ArgumentIdentifier;
    return;
  }
  
  IdentifierInfo *Name = Attr.getArgAsIdent(0)->Ident;
  StringRef Str = Name->getName();

  // Normalize the attribute name, __foo__ becomes foo.
  if (Str.startswith("__") && Str.endswith("__"))
    Str = Str.substr(2, Str.size() - 4);

  unsigned DestWidth = 0;
  bool IntegerMode = true;
  bool ComplexMode = false;
  switch (Str.size()) {
  case 2:
    switch (Str[0]) {
    case 'Q': DestWidth = 8; break;
    case 'H': DestWidth = 16; break;
    case 'S': DestWidth = 32; break;
    case 'D': DestWidth = 64; break;
    case 'X': DestWidth = 96; break;
    case 'T': DestWidth = 128; break;
    }
    if (Str[1] == 'F') {
      IntegerMode = false;
    } else if (Str[1] == 'C') {
      IntegerMode = false;
      ComplexMode = true;
    } else if (Str[1] != 'I') {
      DestWidth = 0;
    }
    break;
  case 4:
    // FIXME: glibc uses 'word' to define register_t; this is narrower than a
    // pointer on PIC16 and other embedded platforms.
    if (Str == "word")
      DestWidth = S.Context.getTargetInfo().getPointerWidth(0);
    else if (Str == "byte")
      DestWidth = S.Context.getTargetInfo().getCharWidth();
    break;
  case 7:
    if (Str == "pointer")
      DestWidth = S.Context.getTargetInfo().getPointerWidth(0);
    break;
  case 11:
    if (Str == "unwind_word")
      DestWidth = S.Context.getTargetInfo().getUnwindWordWidth();
    break;
  }

  QualType OldTy;
  if (TypedefNameDecl *TD = dyn_cast<TypedefNameDecl>(D))
    OldTy = TD->getUnderlyingType();
  else if (ValueDecl *VD = dyn_cast<ValueDecl>(D))
    OldTy = VD->getType();
  else {
    S.Diag(D->getLocation(), diag::err_attr_wrong_decl)
      << "mode" << Attr.getRange();
    return;
  }

  if (!OldTy->getAs<BuiltinType>() && !OldTy->isComplexType())
    S.Diag(Attr.getLoc(), diag::err_mode_not_primitive);
  else if (IntegerMode) {
    if (!OldTy->isIntegralOrEnumerationType())
      S.Diag(Attr.getLoc(), diag::err_mode_wrong_type);
  } else if (ComplexMode) {
    if (!OldTy->isComplexType())
      S.Diag(Attr.getLoc(), diag::err_mode_wrong_type);
  } else {
    if (!OldTy->isFloatingType())
      S.Diag(Attr.getLoc(), diag::err_mode_wrong_type);
  }

  // FIXME: Sync this with InitializePredefinedMacros; we need to match int8_t
  // and friends, at least with glibc.
  // FIXME: Make sure floating-point mappings are accurate
  // FIXME: Support XF and TF types
  if (!DestWidth) {
    S.Diag(Attr.getLoc(), diag::err_unknown_machine_mode) << Name;
    return;
  }

  QualType NewTy;

  if (IntegerMode)
    NewTy = S.Context.getIntTypeForBitwidth(DestWidth,
                                            OldTy->isSignedIntegerType());
  else
    NewTy = S.Context.getRealTypeForBitwidth(DestWidth);

  if (NewTy.isNull()) {
    S.Diag(Attr.getLoc(), diag::err_unsupported_machine_mode) << Name;
    return;
  }

  if (ComplexMode) {
    NewTy = S.Context.getComplexType(NewTy);
  }

  // Install the new type.
  if (TypedefNameDecl *TD = dyn_cast<TypedefNameDecl>(D))
    TD->setModedTypeSourceInfo(TD->getTypeSourceInfo(), NewTy);
  else
    cast<ValueDecl>(D)->setType(NewTy);

  D->addAttr(::new (S.Context)
             ModeAttr(Attr.getRange(), S.Context, Name,
                      Attr.getAttributeSpellingListIndex()));
}

static void handleNoDebugAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (const VarDecl *VD = dyn_cast<VarDecl>(D)) {
    if (!VD->hasGlobalStorage())
      S.Diag(Attr.getLoc(),
             diag::warn_attribute_requires_functions_or_static_globals)
        << Attr.getName();
  } else if (!isFunctionOrMethod(D)) {
    S.Diag(Attr.getLoc(),
           diag::warn_attribute_requires_functions_or_static_globals)
      << Attr.getName();
    return;
  }

  D->addAttr(::new (S.Context)
             NoDebugAttr(Attr.getRange(), S.Context,
                         Attr.getAttributeSpellingListIndex()));
}

static void handleGlobalAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  FunctionDecl *FD = cast<FunctionDecl>(D);
  if (!FD->getResultType()->isVoidType()) {
    TypeLoc TL = FD->getTypeSourceInfo()->getTypeLoc().IgnoreParens();
    if (FunctionTypeLoc FTL = TL.getAs<FunctionTypeLoc>()) {
      S.Diag(FD->getTypeSpecStartLoc(), diag::err_kern_type_not_void_return)
        << FD->getType()
        << FixItHint::CreateReplacement(FTL.getResultLoc().getSourceRange(),
                                        "void");
    } else {
      S.Diag(FD->getTypeSpecStartLoc(), diag::err_kern_type_not_void_return)
        << FD->getType();
    }
    return;
  }

  D->addAttr(::new (S.Context)
              CUDAGlobalAttr(Attr.getRange(), S.Context,
                            Attr.getAttributeSpellingListIndex()));
}

static void handleGNUInlineAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  FunctionDecl *Fn = cast<FunctionDecl>(D);
  if (!Fn->isInlineSpecified()) {
    S.Diag(Attr.getLoc(), diag::warn_gnu_inline_attribute_requires_inline);
    return;
  }

  D->addAttr(::new (S.Context)
             GNUInlineAttr(Attr.getRange(), S.Context,
                           Attr.getAttributeSpellingListIndex()));
}

static void handleCallConvAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (hasDeclarator(D)) return;

  const FunctionDecl *FD = dyn_cast<FunctionDecl>(D);
  // Diagnostic is emitted elsewhere: here we store the (valid) Attr
  // in the Decl node for syntactic reasoning, e.g., pretty-printing.
  CallingConv CC;
  if (S.CheckCallingConvAttr(Attr, CC, FD))
    return;

  if (!isa<ObjCMethodDecl>(D)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedFunctionOrMethod;
    return;
  }

  switch (Attr.getKind()) {
  case AttributeList::AT_FastCall:
    D->addAttr(::new (S.Context)
               FastCallAttr(Attr.getRange(), S.Context,
                            Attr.getAttributeSpellingListIndex()));
    return;
  case AttributeList::AT_StdCall:
    D->addAttr(::new (S.Context)
               StdCallAttr(Attr.getRange(), S.Context,
                           Attr.getAttributeSpellingListIndex()));
    return;
  case AttributeList::AT_ThisCall:
    D->addAttr(::new (S.Context)
               ThisCallAttr(Attr.getRange(), S.Context,
                            Attr.getAttributeSpellingListIndex()));
    return;
  case AttributeList::AT_CDecl:
    D->addAttr(::new (S.Context)
               CDeclAttr(Attr.getRange(), S.Context,
                         Attr.getAttributeSpellingListIndex()));
    return;
  case AttributeList::AT_Pascal:
    D->addAttr(::new (S.Context)
               PascalAttr(Attr.getRange(), S.Context,
                          Attr.getAttributeSpellingListIndex()));
    return;
  case AttributeList::AT_MSABI:
    D->addAttr(::new (S.Context)
               MSABIAttr(Attr.getRange(), S.Context,
                         Attr.getAttributeSpellingListIndex()));
    return;
  case AttributeList::AT_SysVABI:
    D->addAttr(::new (S.Context)
               SysVABIAttr(Attr.getRange(), S.Context,
                           Attr.getAttributeSpellingListIndex()));
    return;
  case AttributeList::AT_Pcs: {
    PcsAttr::PCSType PCS;
    switch (CC) {
    case CC_AAPCS:
      PCS = PcsAttr::AAPCS;
      break;
    case CC_AAPCS_VFP:
      PCS = PcsAttr::AAPCS_VFP;
      break;
    default:
      llvm_unreachable("unexpected calling convention in pcs attribute");
    }

    D->addAttr(::new (S.Context)
               PcsAttr(Attr.getRange(), S.Context, PCS,
                       Attr.getAttributeSpellingListIndex()));
    return;
  }
  case AttributeList::AT_PnaclCall:
    D->addAttr(::new (S.Context)
               PnaclCallAttr(Attr.getRange(), S.Context,
                             Attr.getAttributeSpellingListIndex()));
    return;
  case AttributeList::AT_IntelOclBicc:
    D->addAttr(::new (S.Context)
               IntelOclBiccAttr(Attr.getRange(), S.Context,
                                Attr.getAttributeSpellingListIndex()));
    return;

  default:
    llvm_unreachable("unexpected attribute kind");
  }
}

static void handleOpenCLImageAccessAttr(Sema &S, Decl *D,
                                        const AttributeList &Attr) {
  uint32_t ArgNum;
  if (!checkUInt32Argument(S, Attr, Attr.getArgAsExpr(0), ArgNum))
    return;

  D->addAttr(::new (S.Context) OpenCLImageAccessAttr(Attr.getRange(),
                                                     S.Context, ArgNum));
}

bool Sema::CheckCallingConvAttr(const AttributeList &attr, CallingConv &CC, 
                                const FunctionDecl *FD) {
  if (attr.isInvalid())
    return true;

  unsigned ReqArgs = attr.getKind() == AttributeList::AT_Pcs ? 1 : 0;
  if (!checkAttributeNumArgs(*this, attr, ReqArgs)) {
    attr.setInvalid();
    return true;
  }

  // TODO: diagnose uses of these conventions on the wrong target. Or, better
  // move to TargetAttributesSema one day.
  switch (attr.getKind()) {
  case AttributeList::AT_CDecl: CC = CC_C; break;
  case AttributeList::AT_FastCall: CC = CC_X86FastCall; break;
  case AttributeList::AT_StdCall: CC = CC_X86StdCall; break;
  case AttributeList::AT_ThisCall: CC = CC_X86ThisCall; break;
  case AttributeList::AT_Pascal: CC = CC_X86Pascal; break;
  case AttributeList::AT_MSABI:
    CC = Context.getTargetInfo().getTriple().isOSWindows() ? CC_C :
                                                             CC_X86_64Win64;
    break;
  case AttributeList::AT_SysVABI:
    CC = Context.getTargetInfo().getTriple().isOSWindows() ? CC_X86_64SysV :
                                                             CC_C;
    break;
  case AttributeList::AT_Pcs: {
    StringRef StrRef;
    if (!checkStringLiteralArgumentAttr(attr, 0, StrRef)) {
      attr.setInvalid();
      return true;
    }
    if (StrRef == "aapcs") {
      CC = CC_AAPCS;
      break;
    } else if (StrRef == "aapcs-vfp") {
      CC = CC_AAPCS_VFP;
      break;
    }

    attr.setInvalid();
    Diag(attr.getLoc(), diag::err_invalid_pcs);
    return true;
  }
  case AttributeList::AT_PnaclCall: CC = CC_PnaclCall; break;
  case AttributeList::AT_IntelOclBicc: CC = CC_IntelOclBicc; break;
  default: llvm_unreachable("unexpected attribute kind");
  }

  const TargetInfo &TI = Context.getTargetInfo();
  TargetInfo::CallingConvCheckResult A = TI.checkCallingConvention(CC);
  if (A == TargetInfo::CCCR_Warning) {
    Diag(attr.getLoc(), diag::warn_cconv_ignored) << attr.getName();

    TargetInfo::CallingConvMethodType MT = TargetInfo::CCMT_Unknown;
    if (FD)
      MT = FD->isCXXInstanceMember() ? TargetInfo::CCMT_Member : 
                                    TargetInfo::CCMT_NonMember;
    CC = TI.getDefaultCallingConv(MT);
  }

  return false;
}

static void handleRegparmAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (hasDeclarator(D)) return;

  unsigned numParams;
  if (S.CheckRegparmAttr(Attr, numParams))
    return;

  if (!isa<ObjCMethodDecl>(D)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedFunctionOrMethod;
    return;
  }

  D->addAttr(::new (S.Context)
             RegparmAttr(Attr.getRange(), S.Context, numParams,
                         Attr.getAttributeSpellingListIndex()));
}

/// Checks a regparm attribute, returning true if it is ill-formed and
/// otherwise setting numParams to the appropriate value.
bool Sema::CheckRegparmAttr(const AttributeList &Attr, unsigned &numParams) {
  if (Attr.isInvalid())
    return true;

  if (!checkAttributeNumArgs(*this, Attr, 1)) {
    Attr.setInvalid();
    return true;
  }

  uint32_t NP;
  Expr *NumParamsExpr = Attr.getArgAsExpr(0);
  if (!checkUInt32Argument(*this, Attr, NumParamsExpr, NP)) {
    Attr.setInvalid();
    return true;
  }

  if (Context.getTargetInfo().getRegParmMax() == 0) {
    Diag(Attr.getLoc(), diag::err_attribute_regparm_wrong_platform)
      << NumParamsExpr->getSourceRange();
    Attr.setInvalid();
    return true;
  }

  numParams = NP;
  if (numParams > Context.getTargetInfo().getRegParmMax()) {
    Diag(Attr.getLoc(), diag::err_attribute_regparm_invalid_number)
      << Context.getTargetInfo().getRegParmMax() << NumParamsExpr->getSourceRange();
    Attr.setInvalid();
    return true;
  }

  return false;
}

static void handleLaunchBoundsAttr(Sema &S, Decl *D, const AttributeList &Attr){
  // check the attribute arguments.
  if (Attr.getNumArgs() != 1 && Attr.getNumArgs() != 2) {
    // FIXME: 0 is not okay.
    S.Diag(Attr.getLoc(), diag::err_attribute_too_many_arguments) << 2;
    return;
  }

  if (!isFunctionOrMethod(D)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedFunctionOrMethod;
    return;
  }

  uint32_t MaxThreads, MinBlocks;
  if (!checkUInt32Argument(S, Attr, Attr.getArgAsExpr(0), MaxThreads, 1) ||
      !checkUInt32Argument(S, Attr, Attr.getArgAsExpr(1), MinBlocks, 2))
    return;

  D->addAttr(::new (S.Context)
              CUDALaunchBoundsAttr(Attr.getRange(), S.Context,
                                  MaxThreads, MinBlocks,
                                  Attr.getAttributeSpellingListIndex()));
}

static void handleArgumentWithTypeTagAttr(Sema &S, Decl *D,
                                          const AttributeList &Attr) {
  if (!Attr.isArgIdent(0)) {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_n_type)
      << Attr.getName() << /* arg num = */ 1 << AANT_ArgumentIdentifier;
    return;
  }
  
  if (!checkAttributeNumArgs(S, Attr, 3))
    return;

  StringRef AttrName = Attr.getName()->getName();
  IdentifierInfo *ArgumentKind = Attr.getArgAsIdent(0)->Ident;

  if (!isFunctionOrMethod(D) || !hasFunctionProto(D)) {
    S.Diag(Attr.getLoc(), diag::err_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedFunctionOrMethod;
    return;
  }

  uint64_t ArgumentIdx;
  if (!checkFunctionOrMethodArgumentIndex(S, D, AttrName,
                                          Attr.getLoc(), 2,
                                          Attr.getArgAsExpr(1), ArgumentIdx))
    return;

  uint64_t TypeTagIdx;
  if (!checkFunctionOrMethodArgumentIndex(S, D, AttrName,
                                          Attr.getLoc(), 3,
                                          Attr.getArgAsExpr(2), TypeTagIdx))
    return;

  bool IsPointer = (AttrName == "pointer_with_type_tag");
  if (IsPointer) {
    // Ensure that buffer has a pointer type.
    QualType BufferTy = getFunctionOrMethodArgType(D, ArgumentIdx);
    if (!BufferTy->isPointerType()) {
      S.Diag(Attr.getLoc(), diag::err_attribute_pointers_only)
        << Attr.getName();
    }
  }

  D->addAttr(::new (S.Context)
             ArgumentWithTypeTagAttr(Attr.getRange(), S.Context, ArgumentKind,
                                     ArgumentIdx, TypeTagIdx, IsPointer,
                                     Attr.getAttributeSpellingListIndex()));
}

static void handleTypeTagForDatatypeAttr(Sema &S, Decl *D,
                                         const AttributeList &Attr) {
  if (!Attr.isArgIdent(0)) {
    S.Diag(Attr.getLoc(), diag::err_attribute_argument_n_type)
      << Attr.getName() << 1 << AANT_ArgumentIdentifier;
    return;
  }
  
  if (!checkAttributeNumArgs(S, Attr, 1))
    return;

  if (!isa<VarDecl>(D)) {
    S.Diag(Attr.getLoc(), diag::err_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedVariable;
    return;
  }

  IdentifierInfo *PointerKind = Attr.getArgAsIdent(0)->Ident;
  TypeSourceInfo *MatchingCTypeLoc = 0;
  S.GetTypeFromParser(Attr.getMatchingCType(), &MatchingCTypeLoc);
  assert(MatchingCTypeLoc && "no type source info for attribute argument");

  D->addAttr(::new (S.Context)
             TypeTagForDatatypeAttr(Attr.getRange(), S.Context, PointerKind,
                                    MatchingCTypeLoc,
                                    Attr.getLayoutCompatible(),
                                    Attr.getMustBeNull(),
                                    Attr.getAttributeSpellingListIndex()));
}

//===----------------------------------------------------------------------===//
// Checker-specific attribute handlers.
//===----------------------------------------------------------------------===//

static bool isValidSubjectOfNSAttribute(Sema &S, QualType type) {
  return type->isDependentType() || 
         type->isObjCObjectPointerType() || 
         S.Context.isObjCNSObjectType(type);
}
static bool isValidSubjectOfCFAttribute(Sema &S, QualType type) {
  return type->isDependentType() || 
         type->isPointerType() || 
         isValidSubjectOfNSAttribute(S, type);
}

static void handleNSConsumedAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  ParmVarDecl *param = cast<ParmVarDecl>(D);
  bool typeOK, cf;

  if (Attr.getKind() == AttributeList::AT_NSConsumed) {
    typeOK = isValidSubjectOfNSAttribute(S, param->getType());
    cf = false;
  } else {
    typeOK = isValidSubjectOfCFAttribute(S, param->getType());
    cf = true;
  }

  if (!typeOK) {
    S.Diag(D->getLocStart(), diag::warn_ns_attribute_wrong_parameter_type)
      << Attr.getRange() << Attr.getName() << cf;
    return;
  }

  if (cf)
    param->addAttr(::new (S.Context)
                   CFConsumedAttr(Attr.getRange(), S.Context,
                                  Attr.getAttributeSpellingListIndex()));
  else
    param->addAttr(::new (S.Context)
                   NSConsumedAttr(Attr.getRange(), S.Context,
                                  Attr.getAttributeSpellingListIndex()));
}

static void handleNSReturnsRetainedAttr(Sema &S, Decl *D,
                                        const AttributeList &Attr) {

  QualType returnType;

  if (ObjCMethodDecl *MD = dyn_cast<ObjCMethodDecl>(D))
    returnType = MD->getResultType();
  else if (S.getLangOpts().ObjCAutoRefCount && hasDeclarator(D) &&
           (Attr.getKind() == AttributeList::AT_NSReturnsRetained))
    return; // ignore: was handled as a type attribute
  else if (ObjCPropertyDecl *PD = dyn_cast<ObjCPropertyDecl>(D))
    returnType = PD->getType();
  else if (FunctionDecl *FD = dyn_cast<FunctionDecl>(D))
    returnType = FD->getResultType();
  else {
    S.Diag(D->getLocStart(), diag::warn_attribute_wrong_decl_type)
        << Attr.getRange() << Attr.getName()
        << ExpectedFunctionOrMethod;
    return;
  }

  bool typeOK;
  bool cf;
  switch (Attr.getKind()) {
  default: llvm_unreachable("invalid ownership attribute");
  case AttributeList::AT_NSReturnsAutoreleased:
  case AttributeList::AT_NSReturnsRetained:
  case AttributeList::AT_NSReturnsNotRetained:
    typeOK = isValidSubjectOfNSAttribute(S, returnType);
    cf = false;
    break;

  case AttributeList::AT_CFReturnsRetained:
  case AttributeList::AT_CFReturnsNotRetained:
    typeOK = isValidSubjectOfCFAttribute(S, returnType);
    cf = true;
    break;
  }

  if (!typeOK) {
    S.Diag(D->getLocStart(), diag::warn_ns_attribute_wrong_return_type)
      << Attr.getRange() << Attr.getName() << isa<ObjCMethodDecl>(D) << cf;
    return;
  }

  switch (Attr.getKind()) {
    default:
      llvm_unreachable("invalid ownership attribute");
    case AttributeList::AT_NSReturnsAutoreleased:
      D->addAttr(::new (S.Context)
                 NSReturnsAutoreleasedAttr(Attr.getRange(), S.Context,
                                           Attr.getAttributeSpellingListIndex()));
      return;
    case AttributeList::AT_CFReturnsNotRetained:
      D->addAttr(::new (S.Context)
                 CFReturnsNotRetainedAttr(Attr.getRange(), S.Context,
                                          Attr.getAttributeSpellingListIndex()));
      return;
    case AttributeList::AT_NSReturnsNotRetained:
      D->addAttr(::new (S.Context)
                 NSReturnsNotRetainedAttr(Attr.getRange(), S.Context,
                                          Attr.getAttributeSpellingListIndex()));
      return;
    case AttributeList::AT_CFReturnsRetained:
      D->addAttr(::new (S.Context)
                 CFReturnsRetainedAttr(Attr.getRange(), S.Context,
                                       Attr.getAttributeSpellingListIndex()));
      return;
    case AttributeList::AT_NSReturnsRetained:
      D->addAttr(::new (S.Context)
                 NSReturnsRetainedAttr(Attr.getRange(), S.Context,
                                       Attr.getAttributeSpellingListIndex()));
      return;
  };
}

static void handleObjCReturnsInnerPointerAttr(Sema &S, Decl *D,
                                              const AttributeList &attr) {
  const int EP_ObjCMethod = 1;
  const int EP_ObjCProperty = 2;
  
  SourceLocation loc = attr.getLoc();
  QualType resultType;
  if (isa<ObjCMethodDecl>(D))
    resultType = cast<ObjCMethodDecl>(D)->getResultType();
  else
    resultType = cast<ObjCPropertyDecl>(D)->getType();

  if (!resultType->isReferenceType() &&
      (!resultType->isPointerType() || resultType->isObjCRetainableType())) {
    S.Diag(D->getLocStart(), diag::warn_ns_attribute_wrong_return_type)
      << SourceRange(loc)
    << attr.getName()
    << (isa<ObjCMethodDecl>(D) ? EP_ObjCMethod : EP_ObjCProperty)
    << /*non-retainable pointer*/ 2;

    // Drop the attribute.
    return;
  }

  D->addAttr(::new (S.Context)
                  ObjCReturnsInnerPointerAttr(attr.getRange(), S.Context,
                                              attr.getAttributeSpellingListIndex()));
}

static void handleObjCRequiresSuperAttr(Sema &S, Decl *D,
                                        const AttributeList &attr) {
  ObjCMethodDecl *method = cast<ObjCMethodDecl>(D);
  
  DeclContext *DC = method->getDeclContext();
  if (const ObjCProtocolDecl *PDecl = dyn_cast_or_null<ObjCProtocolDecl>(DC)) {
    S.Diag(D->getLocStart(), diag::warn_objc_requires_super_protocol)
    << attr.getName() << 0;
    S.Diag(PDecl->getLocation(), diag::note_protocol_decl);
    return;
  }
  if (method->getMethodFamily() == OMF_dealloc) {
    S.Diag(D->getLocStart(), diag::warn_objc_requires_super_protocol)
    << attr.getName() << 1;
    return;
  }
  
  method->addAttr(::new (S.Context)
                  ObjCRequiresSuperAttr(attr.getRange(), S.Context,
                                        attr.getAttributeSpellingListIndex()));
}

static void handleCFAuditedTransferAttr(Sema &S, Decl *D,
                                        const AttributeList &Attr) {
  if (checkAttrMutualExclusion<CFUnknownTransferAttr>(S, D, Attr,
                                                      "cf_unknown_transfer"))
    return;

  D->addAttr(::new (S.Context)
             CFAuditedTransferAttr(Attr.getRange(), S.Context,
                                   Attr.getAttributeSpellingListIndex()));
}

static void handleCFUnknownTransferAttr(Sema &S, Decl *D,
                                        const AttributeList &Attr) {
  if (checkAttrMutualExclusion<CFAuditedTransferAttr>(S, D, Attr,
                                                      "cf_audited_transfer"))
    return;

  D->addAttr(::new (S.Context)
             CFUnknownTransferAttr(Attr.getRange(), S.Context,
             Attr.getAttributeSpellingListIndex()));
}

static void handleNSBridgedAttr(Sema &S, Scope *Sc, Decl *D,
                                const AttributeList &Attr) {
  IdentifierLoc *Parm = Attr.isArgIdent(0) ? Attr.getArgAsIdent(0) : 0;

  // In Objective-C, verify that the type names an Objective-C type.
  // We don't want to check this outside of ObjC because people sometimes
  // do crazy C declarations of Objective-C types.
  if (Parm && S.getLangOpts().ObjC1) {
    // Check for an existing type with this name.
    LookupResult R(S, DeclarationName(Parm->Ident), Parm->Loc,
                   Sema::LookupOrdinaryName);
    if (S.LookupName(R, Sc)) {
      NamedDecl *Target = R.getFoundDecl();
      if (Target && !isa<ObjCInterfaceDecl>(Target)) {
        S.Diag(D->getLocStart(), diag::err_ns_bridged_not_interface);
        S.Diag(Target->getLocStart(), diag::note_declared_at);
      }
    }
  }

  D->addAttr(::new (S.Context)
             NSBridgedAttr(Attr.getRange(), S.Context, Parm ? Parm->Ident : 0,
                           Attr.getAttributeSpellingListIndex()));
}

static void handleObjCBridgeAttr(Sema &S, Scope *Sc, Decl *D,
                                const AttributeList &Attr) {
  IdentifierLoc * Parm = Attr.isArgIdent(0) ? Attr.getArgAsIdent(0) : 0;

  if (!Parm) {
    S.Diag(D->getLocStart(), diag::err_objc_attr_not_id) << Attr.getName() << 0;
    return;
  }
  
  D->addAttr(::new (S.Context)
             ObjCBridgeAttr(Attr.getRange(), S.Context, Parm->Ident,
                           Attr.getAttributeSpellingListIndex()));
}

static void handleObjCBridgeMutableAttr(Sema &S, Scope *Sc, Decl *D,
                                        const AttributeList &Attr) {
  IdentifierLoc * Parm = Attr.isArgIdent(0) ? Attr.getArgAsIdent(0) : 0;
  
  if (!Parm) {
    S.Diag(D->getLocStart(), diag::err_objc_attr_not_id) << Attr.getName() << 0;
    return;
  }
  
  D->addAttr(::new (S.Context)
             ObjCBridgeMutableAttr(Attr.getRange(), S.Context, Parm->Ident,
                            Attr.getAttributeSpellingListIndex()));
}

static void handleObjCDesignatedInitializer(Sema &S, Decl *D,
                                            const AttributeList &Attr) {
  SourceLocation Loc = Attr.getLoc();
  ObjCMethodDecl *Method = cast<ObjCMethodDecl>(D);

  if (Method->getMethodFamily() != OMF_init) {
    S.Diag(D->getLocStart(), diag::err_attr_objc_designated_not_init_family)
    << SourceRange(Loc, Loc);
    return;
  }
  DeclContext *DC = Method->getDeclContext();
  if (!isa<ObjCInterfaceDecl>(DC)) {
    S.Diag(D->getLocStart(), diag::err_attr_objc_designated_not_interface)
    << SourceRange(Loc, Loc);
    return;
  }

  Method->addAttr(::new (S.Context)
                  ObjCDesignatedInitializerAttr(Attr.getRange(), S.Context,
                                         Attr.getAttributeSpellingListIndex()));
}

static void handleObjCOwnershipAttr(Sema &S, Decl *D,
                                    const AttributeList &Attr) {
  if (hasDeclarator(D)) return;

  S.Diag(D->getLocStart(), diag::err_attribute_wrong_decl_type)
    << Attr.getRange() << Attr.getName() << ExpectedVariable;
}

static void handleObjCPreciseLifetimeAttr(Sema &S, Decl *D,
                                          const AttributeList &Attr) {
  ValueDecl *vd = cast<ValueDecl>(D);
  QualType type = vd->getType();

  if (!type->isDependentType() &&
      !type->isObjCLifetimeType()) {
    S.Diag(Attr.getLoc(), diag::err_objc_precise_lifetime_bad_type)
      << type;
    return;
  }

  Qualifiers::ObjCLifetime lifetime = type.getObjCLifetime();

  // If we have no lifetime yet, check the lifetime we're presumably
  // going to infer.
  if (lifetime == Qualifiers::OCL_None && !type->isDependentType())
    lifetime = type->getObjCARCImplicitLifetime();

  switch (lifetime) {
  case Qualifiers::OCL_None:
    assert(type->isDependentType() &&
           "didn't infer lifetime for non-dependent type?");
    break;

  case Qualifiers::OCL_Weak:   // meaningful
  case Qualifiers::OCL_Strong: // meaningful
    break;

  case Qualifiers::OCL_ExplicitNone:
  case Qualifiers::OCL_Autoreleasing:
    S.Diag(Attr.getLoc(), diag::warn_objc_precise_lifetime_meaningless)
      << (lifetime == Qualifiers::OCL_Autoreleasing);
    break;
  }

  D->addAttr(::new (S.Context)
             ObjCPreciseLifetimeAttr(Attr.getRange(), S.Context,
                                     Attr.getAttributeSpellingListIndex()));
}

//===----------------------------------------------------------------------===//
// Microsoft specific attribute handlers.
//===----------------------------------------------------------------------===//

static void handleUuidAttr(Sema &S, Decl *D, const AttributeList &Attr) {
  if (!S.LangOpts.CPlusPlus) {
    S.Diag(Attr.getLoc(), diag::err_attribute_not_supported_in_lang)
      << Attr.getName() << AttributeLangSupport::C;
    return;
  }

  if (!isa<CXXRecordDecl>(D)) {
    S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type)
      << Attr.getName() << ExpectedClass;
    return;
  }

  StringRef StrRef;
  SourceLocation LiteralLoc;
  if (!S.checkStringLiteralArgumentAttr(Attr, 0, StrRef, &LiteralLoc))
    return;

  // GUID format is "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX" or
  // "{XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}", normalize to the former.
  if (StrRef.size() == 38 && StrRef.front() == '{' && StrRef.back() == '}')
    StrRef = StrRef.drop_front().drop_back();

  // Validate GUID length.
  if (StrRef.size() != 36) {
    S.Diag(LiteralLoc, diag::err_attribute_uuid_malformed_guid);
    return;
  }

  for (unsigned i = 0; i < 36; ++i) {
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      if (StrRef[i] != '-') {
        S.Diag(LiteralLoc, diag::err_attribute_uuid_malformed_guid);
        return;
      }
    } else if (!isHexDigit(StrRef[i])) {
      S.Diag(LiteralLoc, diag::err_attribute_uuid_malformed_guid);
      return;
    }
  }

  D->addAttr(::new (S.Context) UuidAttr(Attr.getRange(), S.Context, StrRef,
                                        Attr.getAttributeSpellingListIndex()));
}

/// Handles semantic checking for features that are common to all attributes,
/// such as checking whether a parameter was properly specified, or the correct
/// number of arguments were passed, etc.
static bool handleCommonAttributeFeatures(Sema &S, Scope *scope, Decl *D,
                                          const AttributeList &Attr) {
  // Several attributes carry different semantics than the parsing requires, so
  // those are opted out of the common handling.
  //
  // We also bail on unknown and ignored attributes because those are handled
  // as part of the target-specific handling logic.
  if (Attr.hasCustomParsing() ||
      Attr.getKind() == AttributeList::UnknownAttribute ||
      Attr.getKind() == AttributeList::IgnoredAttribute)
    return false;

  // Check whether the attribute requires specific language extensions to be
  // enabled.
  if (!Attr.diagnoseLangOpts(S))
    return true;

  // If there are no optional arguments, then checking for the argument count
  // is trivial.
  if (Attr.getMinArgs() == Attr.getMaxArgs() &&
      !checkAttributeNumArgs(S, Attr, Attr.getMinArgs()))
    return true;

  // Check whether the attribute appertains to the given subject.
  if (!Attr.diagnoseAppertainsTo(S, D))
    return true;

  return false;
}

//===----------------------------------------------------------------------===//
// Top Level Sema Entry Points
//===----------------------------------------------------------------------===//

/// ProcessDeclAttribute - Apply the specific attribute to the specified decl if
/// the attribute applies to decls.  If the attribute is a type attribute, just
/// silently ignore it if a GNU attribute.
static void ProcessDeclAttribute(Sema &S, Scope *scope, Decl *D,
                                 const AttributeList &Attr,
                                 bool IncludeCXX11Attributes) {
  if (Attr.isInvalid())
    return;

  // Ignore C++11 attributes on declarator chunks: they appertain to the type
  // instead.
  if (Attr.isCXX11Attribute() && !IncludeCXX11Attributes)
    return;

  if (handleCommonAttributeFeatures(S, scope, D, Attr))
    return;

  switch (Attr.getKind()) {
  case AttributeList::AT_IBAction:
    handleSimpleAttribute<IBActionAttr>(S, D, Attr); break;
  case AttributeList::AT_IBOutlet:    handleIBOutlet(S, D, Attr); break;
  case AttributeList::AT_IBOutletCollection:
    handleIBOutletCollection(S, D, Attr); break;
  case AttributeList::AT_AddressSpace:
  case AttributeList::AT_ObjCGC:
  case AttributeList::AT_VectorSize:
  case AttributeList::AT_NeonVectorType:
  case AttributeList::AT_NeonPolyVectorType:
  case AttributeList::AT_Ptr32:
  case AttributeList::AT_Ptr64:
  case AttributeList::AT_SPtr:
  case AttributeList::AT_UPtr:
    // Ignore these, these are type attributes, handled by
    // ProcessTypeAttributes.
    break;
  case AttributeList::AT_Alias:       handleAliasAttr       (S, D, Attr); break;
  case AttributeList::AT_Aligned:     handleAlignedAttr     (S, D, Attr); break;
  case AttributeList::AT_AllocSize:   handleAllocSizeAttr   (S, D, Attr); break;
  case AttributeList::AT_AlwaysInline:
    handleSimpleAttribute<AlwaysInlineAttr>(S, D, Attr); break;
  case AttributeList::AT_AnalyzerNoReturn:
    handleAnalyzerNoReturnAttr  (S, D, Attr); break;
  case AttributeList::AT_TLSModel:    handleTLSModelAttr    (S, D, Attr); break;
  case AttributeList::AT_Annotate:    handleAnnotateAttr    (S, D, Attr); break;
  case AttributeList::AT_Availability:handleAvailabilityAttr(S, D, Attr); break;
  case AttributeList::AT_CarriesDependency:
    handleDependencyAttr(S, scope, D, Attr);
    break;
  case AttributeList::AT_Common:      handleCommonAttr      (S, D, Attr); break;
  case AttributeList::AT_CUDAConstant:
  handleSimpleAttribute<CUDAConstantAttr>(S, D, Attr); break;
  case AttributeList::AT_Constructor: handleConstructorAttr (S, D, Attr); break;
  case AttributeList::AT_CXX11NoReturn:
  handleSimpleAttribute<CXX11NoReturnAttr>(S, D, Attr); break;
  case AttributeList::AT_Deprecated:
    handleAttrWithMessage<DeprecatedAttr>(S, D, Attr);
    break;
  case AttributeList::AT_Destructor:  handleDestructorAttr  (S, D, Attr); break;
  case AttributeList::AT_ExtVectorType:
    handleExtVectorTypeAttr(S, scope, D, Attr);
    break;
  case AttributeList::AT_MinSize:
    handleSimpleAttribute<MinSizeAttr>(S, D, Attr);
    break;
  case AttributeList::AT_Format:      handleFormatAttr      (S, D, Attr); break;
  case AttributeList::AT_FormatArg:   handleFormatArgAttr   (S, D, Attr); break;
  case AttributeList::AT_CUDAGlobal:  handleGlobalAttr      (S, D, Attr); break;
  case AttributeList::AT_CUDADevice:
    handleSimpleAttribute<CUDADeviceAttr>(S, D, Attr); break;
  case AttributeList::AT_CUDAHost:
    handleSimpleAttribute<CUDAHostAttr>(S, D, Attr); break;
  case AttributeList::AT_GNUInline:   handleGNUInlineAttr   (S, D, Attr); break;
  case AttributeList::AT_CUDALaunchBounds:
    handleLaunchBoundsAttr(S, D, Attr);
    break;
  case AttributeList::AT_Malloc:      handleMallocAttr      (S, D, Attr); break;
  case AttributeList::AT_MayAlias:
    handleSimpleAttribute<MayAliasAttr>(S, D, Attr); break;
  case AttributeList::AT_Mode:        handleModeAttr        (S, D, Attr); break;
  case AttributeList::AT_NoCommon:
    handleSimpleAttribute<NoCommonAttr>(S, D, Attr); break;
  case AttributeList::AT_NonNull:     handleNonNullAttr     (S, D, Attr); break;
  case AttributeList::AT_Overloadable:
    handleSimpleAttribute<OverloadableAttr>(S, D, Attr); break;
  case AttributeList::AT_Ownership:   handleOwnershipAttr   (S, D, Attr); break;
  case AttributeList::AT_Cold:        handleColdAttr        (S, D, Attr); break;
  case AttributeList::AT_Hot:         handleHotAttr         (S, D, Attr); break;
  case AttributeList::AT_Naked:
    handleSimpleAttribute<NakedAttr>(S, D, Attr); break;
  case AttributeList::AT_NoReturn:    handleNoReturnAttr    (S, D, Attr); break;
  case AttributeList::AT_NoThrow:     handleNothrowAttr     (S, D, Attr); break;
  case AttributeList::AT_CUDAShared:
    handleSimpleAttribute<CUDASharedAttr>(S, D, Attr); break;
  case AttributeList::AT_VecReturn:   handleVecReturnAttr   (S, D, Attr); break;

  case AttributeList::AT_ObjCOwnership:
    handleObjCOwnershipAttr(S, D, Attr); break;
  case AttributeList::AT_ObjCPreciseLifetime:
    handleObjCPreciseLifetimeAttr(S, D, Attr); break;

  case AttributeList::AT_ObjCReturnsInnerPointer:
    handleObjCReturnsInnerPointerAttr(S, D, Attr); break;

  case AttributeList::AT_ObjCRequiresSuper:
      handleObjCRequiresSuperAttr(S, D, Attr); break;
      
  case AttributeList::AT_NSBridged:
    handleNSBridgedAttr(S, scope, D, Attr); break;
      
  case AttributeList::AT_ObjCBridge:
    handleObjCBridgeAttr(S, scope, D, Attr); break;
      
  case AttributeList::AT_ObjCBridgeMutable:
    handleObjCBridgeMutableAttr(S, scope, D, Attr); break;

  case AttributeList::AT_ObjCDesignatedInitializer:
    handleObjCDesignatedInitializer(S, D, Attr); break;

  case AttributeList::AT_CFAuditedTransfer:
    handleCFAuditedTransferAttr(S, D, Attr); break;
  case AttributeList::AT_CFUnknownTransfer:
    handleCFUnknownTransferAttr(S, D, Attr); break;

  // Checker-specific.
  case AttributeList::AT_CFConsumed:
  case AttributeList::AT_NSConsumed:  handleNSConsumedAttr  (S, D, Attr); break;
  case AttributeList::AT_NSConsumesSelf:
    handleSimpleAttribute<NSConsumesSelfAttr>(S, D, Attr); break;

  case AttributeList::AT_NSReturnsAutoreleased:
  case AttributeList::AT_NSReturnsNotRetained:
  case AttributeList::AT_CFReturnsNotRetained:
  case AttributeList::AT_NSReturnsRetained:
  case AttributeList::AT_CFReturnsRetained:
    handleNSReturnsRetainedAttr(S, D, Attr); break;
  case AttributeList::AT_WorkGroupSizeHint:
    handleWorkGroupSize<WorkGroupSizeHintAttr>(S, D, Attr); break;
  case AttributeList::AT_ReqdWorkGroupSize:
    handleWorkGroupSize<ReqdWorkGroupSizeAttr>(S, D, Attr); break;
  case AttributeList::AT_VecTypeHint:
    handleVecTypeHint(S, D, Attr); break;

  case AttributeList::AT_InitPriority: 
      handleInitPriorityAttr(S, D, Attr); break;
      
  case AttributeList::AT_Packed:      handlePackedAttr      (S, D, Attr); break;
  case AttributeList::AT_Section:     handleSectionAttr     (S, D, Attr); break;
  case AttributeList::AT_Unavailable:
    handleAttrWithMessage<UnavailableAttr>(S, D, Attr);
    break;
  case AttributeList::AT_ArcWeakrefUnavailable: 
    handleSimpleAttribute<ArcWeakrefUnavailableAttr>(S, D, Attr); break;
  case AttributeList::AT_ObjCRootClass:
    handleSimpleAttribute<ObjCRootClassAttr>(S, D, Attr); break;
  case AttributeList::AT_ObjCSuppressProtocol:
    handleObjCSuppresProtocolAttr(S, D, Attr);
    break;
  case AttributeList::AT_ObjCRequiresPropertyDefs:
    handleSimpleAttribute<ObjCRequiresPropertyDefsAttr>(S, D, Attr); break;
  case AttributeList::AT_Unused:      handleUnusedAttr      (S, D, Attr); break;
  case AttributeList::AT_ReturnsTwice:
    handleSimpleAttribute<ReturnsTwiceAttr>(S, D, Attr); break;
  case AttributeList::AT_Used:        handleUsedAttr        (S, D, Attr); break;
  case AttributeList::AT_Visibility:
    handleVisibilityAttr(S, D, Attr, false);
    break;
  case AttributeList::AT_TypeVisibility:
    handleVisibilityAttr(S, D, Attr, true);
    break;
  case AttributeList::AT_WarnUnused:
    handleSimpleAttribute<WarnUnusedAttr>(S, D, Attr); break;
  case AttributeList::AT_WarnUnusedResult: handleWarnUnusedResult(S, D, Attr);
    break;
  case AttributeList::AT_Weak:
    handleSimpleAttribute<WeakAttr>(S, D, Attr); break;
  case AttributeList::AT_WeakRef:     handleWeakRefAttr     (S, D, Attr); break;
  case AttributeList::AT_WeakImport:  handleWeakImportAttr  (S, D, Attr); break;
  case AttributeList::AT_TransparentUnion:
    handleTransparentUnionAttr(S, D, Attr);
    break;
  case AttributeList::AT_ObjCException:
    handleSimpleAttribute<ObjCExceptionAttr>(S, D, Attr); break;
  case AttributeList::AT_ObjCMethodFamily:
    handleObjCMethodFamilyAttr(S, D, Attr);
    break;
  case AttributeList::AT_ObjCNSObject:handleObjCNSObject    (S, D, Attr); break;
  case AttributeList::AT_Blocks:      handleBlocksAttr      (S, D, Attr); break;
  case AttributeList::AT_Sentinel:    handleSentinelAttr    (S, D, Attr); break;
  case AttributeList::AT_Const:       handleConstAttr       (S, D, Attr); break;
  case AttributeList::AT_Pure:
    handleSimpleAttribute<PureAttr>(S, D, Attr); break;
  case AttributeList::AT_Cleanup:     handleCleanupAttr     (S, D, Attr); break;
  case AttributeList::AT_NoDebug:     handleNoDebugAttr     (S, D, Attr); break;
  case AttributeList::AT_NoInline:
    handleSimpleAttribute<NoInlineAttr>(S, D, Attr); break;
  case AttributeList::AT_Regparm:     handleRegparmAttr     (S, D, Attr); break;
  case AttributeList::IgnoredAttribute:
    // Just ignore
    break;
  case AttributeList::AT_NoInstrumentFunction:  // Interacts with -pg.
    handleSimpleAttribute<NoInstrumentFunctionAttr>(S, D, Attr); break;
  case AttributeList::AT_StdCall:
  case AttributeList::AT_CDecl:
  case AttributeList::AT_FastCall:
  case AttributeList::AT_ThisCall:
  case AttributeList::AT_Pascal:
  case AttributeList::AT_MSABI:
  case AttributeList::AT_SysVABI:
  case AttributeList::AT_Pcs:
  case AttributeList::AT_PnaclCall:
  case AttributeList::AT_IntelOclBicc:
    handleCallConvAttr(S, D, Attr);
    break;
  case AttributeList::AT_OpenCLKernel:
    handleSimpleAttribute<OpenCLKernelAttr>(S, D, Attr); break;
  case AttributeList::AT_OpenCLImageAccess:
    handleOpenCLImageAccessAttr(S, D, Attr);
    break;

  // Microsoft attributes:
  case AttributeList::AT_MsStruct:
    handleSimpleAttribute<MsStructAttr>(S, D, Attr);
    break;
  case AttributeList::AT_Uuid:
    handleUuidAttr(S, D, Attr);
    break;
  case AttributeList::AT_SingleInheritance:
    handleSimpleAttribute<SingleInheritanceAttr>(S, D, Attr); break;
  case AttributeList::AT_MultipleInheritance:
    handleSimpleAttribute<MultipleInheritanceAttr>(S, D, Attr); break;
  case AttributeList::AT_VirtualInheritance:
    handleSimpleAttribute<VirtualInheritanceAttr>(S, D, Attr); break;
  case AttributeList::AT_Win64:
    handleSimpleAttribute<Win64Attr>(S, D, Attr); break;
  case AttributeList::AT_ForceInline:
    handleSimpleAttribute<ForceInlineAttr>(S, D, Attr); break;
  case AttributeList::AT_SelectAny:
    handleSimpleAttribute<SelectAnyAttr>(S, D, Attr); break;

  // Thread safety attributes:
  case AttributeList::AT_AssertExclusiveLock:
    handleAssertExclusiveLockAttr(S, D, Attr);
    break;
  case AttributeList::AT_AssertSharedLock:
    handleAssertSharedLockAttr(S, D, Attr);
    break;
  case AttributeList::AT_GuardedVar:
    handleSimpleAttribute<GuardedVarAttr>(S, D, Attr); break;
  case AttributeList::AT_PtGuardedVar:
    handlePtGuardedVarAttr(S, D, Attr);
    break;
  case AttributeList::AT_ScopedLockable:
    handleSimpleAttribute<ScopedLockableAttr>(S, D, Attr); break;
  case AttributeList::AT_NoSanitizeAddress:
    handleSimpleAttribute<NoSanitizeAddressAttr>(S, D, Attr);
    break;
  case AttributeList::AT_NoThreadSafetyAnalysis:
    handleSimpleAttribute<NoThreadSafetyAnalysisAttr>(S, D, Attr);
    break;
  case AttributeList::AT_NoSanitizeThread:
    handleSimpleAttribute<NoSanitizeThreadAttr>(S, D, Attr);
    break;
  case AttributeList::AT_NoSanitizeMemory:
    handleSimpleAttribute<NoSanitizeMemoryAttr>(S, D, Attr);
    break;
  case AttributeList::AT_Lockable:
    handleSimpleAttribute<LockableAttr>(S, D, Attr); break;
  case AttributeList::AT_GuardedBy:
    handleGuardedByAttr(S, D, Attr);
    break;
  case AttributeList::AT_PtGuardedBy:
    handlePtGuardedByAttr(S, D, Attr);
    break;
  case AttributeList::AT_ExclusiveLockFunction:
    handleExclusiveLockFunctionAttr(S, D, Attr);
    break;
  case AttributeList::AT_ExclusiveLocksRequired:
    handleExclusiveLocksRequiredAttr(S, D, Attr);
    break;
  case AttributeList::AT_ExclusiveTrylockFunction:
    handleExclusiveTrylockFunctionAttr(S, D, Attr);
    break;
  case AttributeList::AT_LockReturned:
    handleLockReturnedAttr(S, D, Attr);
    break;
  case AttributeList::AT_LocksExcluded:
    handleLocksExcludedAttr(S, D, Attr);
    break;
  case AttributeList::AT_SharedLockFunction:
    handleSharedLockFunctionAttr(S, D, Attr);
    break;
  case AttributeList::AT_SharedLocksRequired:
    handleSharedLocksRequiredAttr(S, D, Attr);
    break;
  case AttributeList::AT_SharedTrylockFunction:
    handleSharedTrylockFunctionAttr(S, D, Attr);
    break;
  case AttributeList::AT_UnlockFunction:
    handleUnlockFunAttr(S, D, Attr);
    break;
  case AttributeList::AT_AcquiredBefore:
    handleAcquiredBeforeAttr(S, D, Attr);
    break;
  case AttributeList::AT_AcquiredAfter:
    handleAcquiredAfterAttr(S, D, Attr);
    break;

  // Consumed analysis attributes.
  case AttributeList::AT_Consumable:
    handleConsumableAttr(S, D, Attr);
    break;
  case AttributeList::AT_CallableWhen:
    handleCallableWhenAttr(S, D, Attr);
    break;
  case AttributeList::AT_ParamTypestate:
    handleParamTypestateAttr(S, D, Attr);
    break;
  case AttributeList::AT_ReturnTypestate:
    handleReturnTypestateAttr(S, D, Attr);
    break;
  case AttributeList::AT_SetTypestate:
    handleSetTypestateAttr(S, D, Attr);
    break;
  case AttributeList::AT_TestTypestate:
    handleTestTypestateAttr(S, D, Attr);
    break;

  // Type safety attributes.
  case AttributeList::AT_ArgumentWithTypeTag:
    handleArgumentWithTypeTagAttr(S, D, Attr);
    break;
  case AttributeList::AT_TypeTagForDatatype:
    handleTypeTagForDatatypeAttr(S, D, Attr);
    break;

  default:
    // Ask target about the attribute.
    const TargetAttributesSema &TargetAttrs = S.getTargetAttributesSema();
    if (!TargetAttrs.ProcessDeclAttribute(scope, D, Attr, S))
      S.Diag(Attr.getLoc(), Attr.isDeclspecAttribute() ? 
             diag::warn_unhandled_ms_attribute_ignored : 
             diag::warn_unknown_attribute_ignored) << Attr.getName();
    break;
  }
}

/// ProcessDeclAttributeList - Apply all the decl attributes in the specified
/// attribute list to the specified decl, ignoring any type attributes.
void Sema::ProcessDeclAttributeList(Scope *S, Decl *D,
                                    const AttributeList *AttrList,
                                    bool IncludeCXX11Attributes) {
  for (const AttributeList* l = AttrList; l; l = l->getNext())
    ProcessDeclAttribute(*this, S, D, *l, IncludeCXX11Attributes);

  // GCC accepts
  // static int a9 __attribute__((weakref));
  // but that looks really pointless. We reject it.
  if (D->hasAttr<WeakRefAttr>() && !D->hasAttr<AliasAttr>()) {
    Diag(AttrList->getLoc(), diag::err_attribute_weakref_without_alias) <<
    cast<NamedDecl>(D)->getNameAsString();
    D->dropAttr<WeakRefAttr>();
    return;
  }
}

// Annotation attributes are the only attributes allowed after an access
// specifier.
bool Sema::ProcessAccessDeclAttributeList(AccessSpecDecl *ASDecl,
                                          const AttributeList *AttrList) {
  for (const AttributeList* l = AttrList; l; l = l->getNext()) {
    if (l->getKind() == AttributeList::AT_Annotate) {
      handleAnnotateAttr(*this, ASDecl, *l);
    } else {
      Diag(l->getLoc(), diag::err_only_annotate_after_access_spec);
      return true;
    }
  }

  return false;
}

/// checkUnusedDeclAttributes - Check a list of attributes to see if it
/// contains any decl attributes that we should warn about.
static void checkUnusedDeclAttributes(Sema &S, const AttributeList *A) {
  for ( ; A; A = A->getNext()) {
    // Only warn if the attribute is an unignored, non-type attribute.
    if (A->isUsedAsTypeAttr() || A->isInvalid()) continue;
    if (A->getKind() == AttributeList::IgnoredAttribute) continue;

    if (A->getKind() == AttributeList::UnknownAttribute) {
      S.Diag(A->getLoc(), diag::warn_unknown_attribute_ignored)
        << A->getName() << A->getRange();
    } else {
      S.Diag(A->getLoc(), diag::warn_attribute_not_on_decl)
        << A->getName() << A->getRange();
    }
  }
}

/// checkUnusedDeclAttributes - Given a declarator which is not being
/// used to build a declaration, complain about any decl attributes
/// which might be lying around on it.
void Sema::checkUnusedDeclAttributes(Declarator &D) {
  ::checkUnusedDeclAttributes(*this, D.getDeclSpec().getAttributes().getList());
  ::checkUnusedDeclAttributes(*this, D.getAttributes());
  for (unsigned i = 0, e = D.getNumTypeObjects(); i != e; ++i)
    ::checkUnusedDeclAttributes(*this, D.getTypeObject(i).getAttrs());
}

/// DeclClonePragmaWeak - clone existing decl (maybe definition),
/// \#pragma weak needs a non-definition decl and source may not have one.
NamedDecl * Sema::DeclClonePragmaWeak(NamedDecl *ND, IdentifierInfo *II,
                                      SourceLocation Loc) {
  assert(isa<FunctionDecl>(ND) || isa<VarDecl>(ND));
  NamedDecl *NewD = 0;
  if (FunctionDecl *FD = dyn_cast<FunctionDecl>(ND)) {
    FunctionDecl *NewFD;
    // FIXME: Missing call to CheckFunctionDeclaration().
    // FIXME: Mangling?
    // FIXME: Is the qualifier info correct?
    // FIXME: Is the DeclContext correct?
    NewFD = FunctionDecl::Create(FD->getASTContext(), FD->getDeclContext(),
                                 Loc, Loc, DeclarationName(II),
                                 FD->getType(), FD->getTypeSourceInfo(),
                                 SC_None, false/*isInlineSpecified*/,
                                 FD->hasPrototype(),
                                 false/*isConstexprSpecified*/);
    NewD = NewFD;

    if (FD->getQualifier())
      NewFD->setQualifierInfo(FD->getQualifierLoc());

    // Fake up parameter variables; they are declared as if this were
    // a typedef.
    QualType FDTy = FD->getType();
    if (const FunctionProtoType *FT = FDTy->getAs<FunctionProtoType>()) {
      SmallVector<ParmVarDecl*, 16> Params;
      for (FunctionProtoType::arg_type_iterator AI = FT->arg_type_begin(),
           AE = FT->arg_type_end(); AI != AE; ++AI) {
        ParmVarDecl *Param = BuildParmVarDeclForTypedef(NewFD, Loc, *AI);
        Param->setScopeInfo(0, Params.size());
        Params.push_back(Param);
      }
      NewFD->setParams(Params);
    }
  } else if (VarDecl *VD = dyn_cast<VarDecl>(ND)) {
    NewD = VarDecl::Create(VD->getASTContext(), VD->getDeclContext(),
                           VD->getInnerLocStart(), VD->getLocation(), II,
                           VD->getType(), VD->getTypeSourceInfo(),
                           VD->getStorageClass());
    if (VD->getQualifier()) {
      VarDecl *NewVD = cast<VarDecl>(NewD);
      NewVD->setQualifierInfo(VD->getQualifierLoc());
    }
  }
  return NewD;
}

/// DeclApplyPragmaWeak - A declaration (maybe definition) needs \#pragma weak
/// applied to it, possibly with an alias.
void Sema::DeclApplyPragmaWeak(Scope *S, NamedDecl *ND, WeakInfo &W) {
  if (W.getUsed()) return; // only do this once
  W.setUsed(true);
  if (W.getAlias()) { // clone decl, impersonate __attribute(weak,alias(...))
    IdentifierInfo *NDId = ND->getIdentifier();
    NamedDecl *NewD = DeclClonePragmaWeak(ND, W.getAlias(), W.getLocation());
    NewD->addAttr(::new (Context) AliasAttr(W.getLocation(), Context,
                                            NDId->getName()));
    NewD->addAttr(::new (Context) WeakAttr(W.getLocation(), Context));
    WeakTopLevelDecl.push_back(NewD);
    // FIXME: "hideous" code from Sema::LazilyCreateBuiltin
    // to insert Decl at TU scope, sorry.
    DeclContext *SavedContext = CurContext;
    CurContext = Context.getTranslationUnitDecl();
    PushOnScopeChains(NewD, S);
    CurContext = SavedContext;
  } else { // just add weak to existing
    ND->addAttr(::new (Context) WeakAttr(W.getLocation(), Context));
  }
}

void Sema::ProcessPragmaWeak(Scope *S, Decl *D) {
  // It's valid to "forward-declare" #pragma weak, in which case we
  // have to do this.
  LoadExternalWeakUndeclaredIdentifiers();
  if (!WeakUndeclaredIdentifiers.empty()) {
    NamedDecl *ND = NULL;
    if (VarDecl *VD = dyn_cast<VarDecl>(D))
      if (VD->isExternC())
        ND = VD;
    if (FunctionDecl *FD = dyn_cast<FunctionDecl>(D))
      if (FD->isExternC())
        ND = FD;
    if (ND) {
      if (IdentifierInfo *Id = ND->getIdentifier()) {
        llvm::DenseMap<IdentifierInfo*,WeakInfo>::iterator I
          = WeakUndeclaredIdentifiers.find(Id);
        if (I != WeakUndeclaredIdentifiers.end()) {
          WeakInfo W = I->second;
          DeclApplyPragmaWeak(S, ND, W);
          WeakUndeclaredIdentifiers[Id] = W;
        }
      }
    }
  }
}

/// ProcessDeclAttributes - Given a declarator (PD) with attributes indicated in
/// it, apply them to D.  This is a bit tricky because PD can have attributes
/// specified in many different places, and we need to find and apply them all.
void Sema::ProcessDeclAttributes(Scope *S, Decl *D, const Declarator &PD) {
  // Apply decl attributes from the DeclSpec if present.
  if (const AttributeList *Attrs = PD.getDeclSpec().getAttributes().getList())
    ProcessDeclAttributeList(S, D, Attrs);

  // Walk the declarator structure, applying decl attributes that were in a type
  // position to the decl itself.  This handles cases like:
  //   int *__attr__(x)** D;
  // when X is a decl attribute.
  for (unsigned i = 0, e = PD.getNumTypeObjects(); i != e; ++i)
    if (const AttributeList *Attrs = PD.getTypeObject(i).getAttrs())
      ProcessDeclAttributeList(S, D, Attrs, /*IncludeCXX11Attributes=*/false);

  // Finally, apply any attributes on the decl itself.
  if (const AttributeList *Attrs = PD.getAttributes())
    ProcessDeclAttributeList(S, D, Attrs);
}

/// Is the given declaration allowed to use a forbidden type?
static bool isForbiddenTypeAllowed(Sema &S, Decl *decl) {
  // Private ivars are always okay.  Unfortunately, people don't
  // always properly make their ivars private, even in system headers.
  // Plus we need to make fields okay, too.
  // Function declarations in sys headers will be marked unavailable.
  if (!isa<FieldDecl>(decl) && !isa<ObjCPropertyDecl>(decl) &&
      !isa<FunctionDecl>(decl))
    return false;

  // Require it to be declared in a system header.
  return S.Context.getSourceManager().isInSystemHeader(decl->getLocation());
}

/// Handle a delayed forbidden-type diagnostic.
static void handleDelayedForbiddenType(Sema &S, DelayedDiagnostic &diag,
                                       Decl *decl) {
  if (decl && isForbiddenTypeAllowed(S, decl)) {
    decl->addAttr(new (S.Context) UnavailableAttr(diag.Loc, S.Context,
                        "this system declaration uses an unsupported type"));
    return;
  }
  if (S.getLangOpts().ObjCAutoRefCount)
    if (const FunctionDecl *FD = dyn_cast<FunctionDecl>(decl)) {
      // FIXME: we may want to suppress diagnostics for all
      // kind of forbidden type messages on unavailable functions. 
      if (FD->hasAttr<UnavailableAttr>() &&
          diag.getForbiddenTypeDiagnostic() == 
          diag::err_arc_array_param_no_ownership) {
        diag.Triggered = true;
        return;
      }
    }

  S.Diag(diag.Loc, diag.getForbiddenTypeDiagnostic())
    << diag.getForbiddenTypeOperand() << diag.getForbiddenTypeArgument();
  diag.Triggered = true;
}

void Sema::PopParsingDeclaration(ParsingDeclState state, Decl *decl) {
  assert(DelayedDiagnostics.getCurrentPool());
  DelayedDiagnosticPool &poppedPool = *DelayedDiagnostics.getCurrentPool();
  DelayedDiagnostics.popWithoutEmitting(state);

  // When delaying diagnostics to run in the context of a parsed
  // declaration, we only want to actually emit anything if parsing
  // succeeds.
  if (!decl) return;

  // We emit all the active diagnostics in this pool or any of its
  // parents.  In general, we'll get one pool for the decl spec
  // and a child pool for each declarator; in a decl group like:
  //   deprecated_typedef foo, *bar, baz();
  // only the declarator pops will be passed decls.  This is correct;
  // we really do need to consider delayed diagnostics from the decl spec
  // for each of the different declarations.
  const DelayedDiagnosticPool *pool = &poppedPool;
  do {
    for (DelayedDiagnosticPool::pool_iterator
           i = pool->pool_begin(), e = pool->pool_end(); i != e; ++i) {
      // This const_cast is a bit lame.  Really, Triggered should be mutable.
      DelayedDiagnostic &diag = const_cast<DelayedDiagnostic&>(*i);
      if (diag.Triggered)
        continue;

      switch (diag.Kind) {
      case DelayedDiagnostic::Deprecation:
        // Don't bother giving deprecation diagnostics if the decl is invalid.
        if (!decl->isInvalidDecl())
          HandleDelayedDeprecationCheck(diag, decl);
        break;

      case DelayedDiagnostic::Access:
        HandleDelayedAccessCheck(diag, decl);
        break;

      case DelayedDiagnostic::ForbiddenType:
        handleDelayedForbiddenType(*this, diag, decl);
        break;
      }
    }
  } while ((pool = pool->getParent()));
}

/// Given a set of delayed diagnostics, re-emit them as if they had
/// been delayed in the current context instead of in the given pool.
/// Essentially, this just moves them to the current pool.
void Sema::redelayDiagnostics(DelayedDiagnosticPool &pool) {
  DelayedDiagnosticPool *curPool = DelayedDiagnostics.getCurrentPool();
  assert(curPool && "re-emitting in undelayed context not supported");
  curPool->steal(pool);
}

static bool isDeclDeprecated(Decl *D) {
  do {
    if (D->isDeprecated())
      return true;
    // A category implicitly has the availability of the interface.
    if (const ObjCCategoryDecl *CatD = dyn_cast<ObjCCategoryDecl>(D))
      return CatD->getClassInterface()->isDeprecated();
  } while ((D = cast_or_null<Decl>(D->getDeclContext())));
  return false;
}

static void
DoEmitDeprecationWarning(Sema &S, const NamedDecl *D, StringRef Message,
                         SourceLocation Loc,
                         const ObjCInterfaceDecl *UnknownObjCClass,
                         const ObjCPropertyDecl *ObjCPropery) {
  DeclarationName Name = D->getDeclName();
  if (!Message.empty()) {
    S.Diag(Loc, diag::warn_deprecated_message) << Name << Message;
    S.Diag(D->getLocation(),
           isa<ObjCMethodDecl>(D) ? diag::note_method_declared_at
                                  : diag::note_previous_decl) << Name;
    if (ObjCPropery)
      S.Diag(ObjCPropery->getLocation(), diag::note_property_attribute)
        << ObjCPropery->getDeclName() << 0;
  } else if (!UnknownObjCClass) {
    S.Diag(Loc, diag::warn_deprecated) << D->getDeclName();
    S.Diag(D->getLocation(),
           isa<ObjCMethodDecl>(D) ? diag::note_method_declared_at
                                  : diag::note_previous_decl) << Name;
    if (ObjCPropery)
      S.Diag(ObjCPropery->getLocation(), diag::note_property_attribute)
        << ObjCPropery->getDeclName() << 0;
  } else {
    S.Diag(Loc, diag::warn_deprecated_fwdclass_message) << Name;
    S.Diag(UnknownObjCClass->getLocation(), diag::note_forward_class);
  }
}

void Sema::HandleDelayedDeprecationCheck(DelayedDiagnostic &DD,
                                         Decl *Ctx) {
  if (isDeclDeprecated(Ctx))
    return;

  DD.Triggered = true;
  DoEmitDeprecationWarning(*this, DD.getDeprecationDecl(),
                           DD.getDeprecationMessage(), DD.Loc,
                           DD.getUnknownObjCClass(),
                           DD.getObjCProperty());
}

void Sema::EmitDeprecationWarning(NamedDecl *D, StringRef Message,
                                  SourceLocation Loc,
                                  const ObjCInterfaceDecl *UnknownObjCClass,
                                  const ObjCPropertyDecl  *ObjCProperty) {
  // Delay if we're currently parsing a declaration.
  if (DelayedDiagnostics.shouldDelayDiagnostics()) {
    DelayedDiagnostics.add(DelayedDiagnostic::makeDeprecation(Loc, D, 
                                                              UnknownObjCClass,
                                                              ObjCProperty,
                                                              Message));
    return;
  }

  // Otherwise, don't warn if our current context is deprecated.
  if (isDeclDeprecated(cast<Decl>(getCurLexicalContext())))
    return;
  DoEmitDeprecationWarning(*this, D, Message, Loc, UnknownObjCClass, ObjCProperty);
}
