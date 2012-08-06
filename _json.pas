{******************************************************************************}
{                                                                              }
{          Author: Peb Ruswono Aryan                                           }
{                                                                              }
{******************************************************************************}
unit _json;

interface

uses
  SysUtils;

const
  DIGITS: set of AnsiChar = ['0'..'9'];

type
  TJSONParser = class
  protected
    FStr: string;
    FIdx: integer;
    FCurrentChar: AnsiChar;
    FValue: string;

    procedure Reset;
    procedure Next;
    function HasNext: boolean;

    procedure SkipWhite;

    procedure Unicode;
    procedure ObjToken;
    function PairToken: boolean;
    function ValueToken: boolean;
    procedure StringToken;
    procedure ArrayToken;
    procedure NumberToken;
  public
    constructor Create(JSONstr: string);

    procedure Parse;
  end;

implementation

{ TJSONParser }

procedure TJSONParser.ArrayToken;
var
  first: boolean;
  items: string;
  itemidx: integer;
begin
  items := '';
  itemidx := 0;
  Next;
  first := true;
  SkipWhite;
  repeat
    case FCurrentChar of
      ']': begin
          Next;
          break;
        end;
      '-', '0'..'9': begin
          first := false;
          NumberToken;
          items := items + format('%d : %s', [itemidx, FValue]);
          inc(itemidx);
        end;
      '"': begin
          first := false;
          stringtoken;
          items := items + format('%d : %s', [itemidx, FValue]);
          inc(itemidx);
        end;
      '{': begin
          first := false;
          objtoken;
          items := items + format('%d : %s', [itemidx, FValue]);
          inc(itemidx);
        end;
      ',':
        if not first then begin
          next;
          items := items + ', ';
          skipwhite;
        end
        else
          break;
    else
      break;
    end;
    skipwhite;
  until not hasnext;
  FValue := '<array items=' + items + '>';
end;

constructor TJSONParser.Create(JSONstr: string);
begin
  FStr := JSONstr;
end;

function TJSONParser.hasnext: boolean;
begin
  result := FIdx < length(FStr);
end;

procedure TJSONParser.next;
begin
  inc(FIdx);
  FCurrentChar := FStr[FIdx];
end;

procedure TJSONParser.numbertoken;
var
  start: integer;
begin
  start := FIdx;
  if FCurrentChar = '-' then begin
    next;
  end;

  case FCurrentChar of
    '0': begin
        next;
      end;

    '1'..'9': begin
        while hasnext and (FCurrentChar in DIGITS) do begin
          next;
        end;
      end;
  end;

  if FCurrentChar = '.' then begin
    //read mantissa
    next;
    while hasnext and (FCurrentChar in DIGITS) do begin
      next;
    end;
  end;
  if FCurrentChar in ['e', 'E'] then begin
    next;
    if FCurrentChar in ['+', '-'] then next;
    while hasnext and (FCurrentChar in DIGITS) do begin
      next;
    end;
  end;
  FValue := copy(FStr, start, FIdx - start);
end;

procedure TJSONParser.objtoken;
var
  first: boolean;
begin
  Next;
  first := true;
  SkipWhite;
  repeat
    case FCurrentChar of
      '}': begin
          Next;
          break;
        end;
      '"': begin
          first := false;
          if not PairToken then break;
        end;
      ',':
        if not first then begin
          Next;
          SkipWhite;
          PairToken;
        end
        else
          break;
    else
      break;
    end;
    SkipWhite;
  until not hasnext;
  FValue := '<object>';
end;

function TJSONParser.PairToken: boolean;
var
  pairleft: string;
  pairright: string;
begin
  result := true;

  StringToken;
  pairleft := FValue;
  SkipWhite;

  if FCurrentChar = ':' then
    Next
  else begin
    Next;
    exit;
  end;

  SkipWhite;
  if not ValueToken then begin
    result := false;
    exit;
  end;
  pairright := FValue;
  SkipWhite;
end;

procedure TJSONParser.Parse;
begin
  Reset;
  SkipWhite;
  ObjToken;
end;

procedure TJSONParser.Reset;
begin
  FIdx := 1;
  FCurrentChar := FStr[FIdx];
end;

procedure TJSONParser.SkipWhite;
begin
  while hasnext and (FCurrentChar in [' ', #9, #10, #13]) do
    next;
end;

procedure TJSONParser.StringToken;
var
  value: string;
begin
  value := '';
  repeat
    Next;
    case FCurrentChar of
      '"': begin
          Next;
          break;
        end;
      '\': begin
          Next;
          case FCurrentChar of
            '"': begin
                value := value + '"';
                Next;
              end;
            '\': begin
                value := value + '\';
                Next;
              end;
            '/': begin
                value := value + '/';
                Next;
              end;
            'b': begin
                value := copy(value, 1, length(value) - 1);
                Next;
              end;
            'f': begin
                Next;
              end;
            'n': Next;
            'r': begin
                value := value + #13;
                Next;
              end;
            't': begin
                value := value + #9;
                next;
              end;
            'u': Unicode;
          else
            break;
          end;
        end;
    else
      value := value + FCurrentChar;
    end;
  until not HasNext;
  FValue := value;
end;

procedure TJSONParser.Unicode;
begin
  //ignore codes
  Next; //u
  Next;
  Next;
  Next;
  Next;
end;

function TJSONParser.ValueToken: boolean;
begin
  FValue := '';
  result := true;
  case FCurrentChar of
    '"': StringToken;
    '{': ObjToken;
    '[': ArrayToken;
    '-', '0'..'9': NumberToken;
  else
    result := false;
    exit;
  end;
end;

end.

