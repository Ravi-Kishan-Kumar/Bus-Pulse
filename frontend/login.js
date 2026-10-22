const $ = (selector) => document.querySelector(selector);
let role = 'commuter';
let creating = false;

function setMessage(message, error = false) {
  const node = $('#auth-message');
  node.textContent = message;
  node.classList.toggle('error', error);
}

function selectRole(nextRole) {
  role = nextRole;
  creating = false;
  document.querySelectorAll('.login-role').forEach((button) => {
    button.classList.toggle('selected', button.dataset.role === role);
  });
  $('#display-name-wrap').hidden = role !== 'commuter';
  $('#register-prompt').hidden = role !== 'commuter';
  $('#admin-hint').hidden = role !== 'admin';
  $('#submit-auth').textContent = `Sign in as ${role === 'admin' ? 'administrator' : 'commuter'}`;
  $('#password').autocomplete = 'current-password';
  $('#password').minLength = role === 'admin' ? 1 : 8;
  $('#register-copy').textContent = 'New to Bus Pulse?';
  $('#register-button').textContent = 'Create commuter account';
  setMessage('');
}

document.querySelectorAll('.login-role').forEach((button) => button.addEventListener('click', () => selectRole(button.dataset.role)));
$('#register-button').addEventListener('click', () => {
  creating = !creating;
  $('#submit-auth').textContent = creating ? 'Create commuter account' : 'Sign in as commuter';
  $('#password').autocomplete = creating ? 'new-password' : 'current-password';
  $('#password').minLength = 8;
  $('#register-copy').textContent = creating ? 'Already registered?' : 'New to Bus Pulse?';
  $('#register-button').textContent = creating ? 'Sign in instead' : 'Create commuter account';
  setMessage(creating ? 'Create an account with a password of at least 8 characters.' : 'Sign in with your commuter account.');
});

$('#login-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const button = $('#submit-auth');
  const body = {
    username: $('#username').value.trim(),
    password: $('#password').value,
    role,
    display_name: $('#display-name').value.trim(),
  };
  button.disabled = true;
  setMessage(creating ? 'Creating your account…' : 'Signing in…');
  try {
    let response;
    if (creating) {
      response = await fetch('/api/auth/register', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      if (!response.ok) throw new Error((await response.json().catch(() => ({}))).detail || 'Could not create account');
      creating = false;
    }
    response = await fetch('/api/auth/login', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    if (!response.ok) throw new Error((await response.json().catch(() => ({}))).detail || 'Sign-in failed');
    location.assign('/');
  } catch (error) {
    setMessage(error.message, true);
    button.disabled = false;
  }
});

selectRole('commuter');
